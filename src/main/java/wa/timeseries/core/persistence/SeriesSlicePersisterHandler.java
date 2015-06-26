package wa.timeseries.core.persistence;

import com.google.appengine.api.datastore.*;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import wa.timeseries.core.OffsetValue;
import wa.timeseries.core.SeriesSlice;
import wa.timeseries.core.persistence.serializer.IValueSerializer;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Persister handler for Series Slices in AppEngine
 */
public class SeriesSlicePersisterHandler<T>
{

  public static final String SEQ = "seq";

  public static final String MAX_SIZE = "maxSize";

  public static final String MAX_RESOLUTION = "maxResolution";

  public static final String LAST_UPDATE = "lastUpdate";

  private static final String VALUE = "value";

  public static final String TS_SLICE = "TSSlice";

  private final IValueSerializer<T> serializer;

  public SeriesSlicePersisterHandler(IValueSerializer<T> serializer)
  {
    this.serializer = serializer;
  }

  /**
   * TODO Add Transaction support
   * @param slice
   */
  public Key persist(final SeriesSlice<T> slice)
  {
    final DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();

    Entity entity = null;

    if (slice instanceof SeriesSliceWrapper)
    {
      SeriesSliceWrapper wrapper = (SeriesSliceWrapper) slice;
      entity = new Entity(TS_SLICE, wrapper.getId());
    }
    else
    {
      entity = new Entity(TS_SLICE);
    }

    ArrayList<Object> values = new ArrayList<>();
    SeriesSlice<T>.SliceIterator iterator = slice.iterator();

    //The iterator is sorted. The serialized values will endup in the right
    //place in the values array.
    while(iterator.hasNext())
    {
      OffsetValue<T> offsetValue = iterator.next();
      Object serialized = serializer.serialize(offsetValue.getValue());
      values.add(serialized);
    }

    entity.setProperty(SEQ, slice.getSeq());
    entity.setProperty(MAX_SIZE, slice.getMaxSize());
    entity.setProperty(MAX_RESOLUTION, slice.getMaxResolution());
    entity.setProperty(LAST_UPDATE, new Date());
    entity.setProperty(VALUE, values);

    return datastore.put(entity);
  }

  public List<SeriesSlice<T>> query(long seqStart, long seqEnd)
  {
    Filter startFilter =
        new FilterPredicate(SEQ,
            FilterOperator.GREATER_THAN_OR_EQUAL,
            seqStart);
    Filter endFilter =
        new FilterPredicate(SEQ,
            FilterOperator.LESS_THAN_OR_EQUAL,
            seqEnd);

    Filter compFilter = Query.CompositeFilterOperator.and(startFilter, endFilter);

    Query q = new Query(TS_SLICE).setFilter(compFilter);
    Iterable<Entity> iterable =
        DatastoreServiceFactory.getDatastoreService().prepare(q).asIterable();

    ArrayList<SeriesSlice<T>> result = new ArrayList<>();

    for (Entity entity : iterable)
    {
      result.add(fromEntity(entity));
    }

    return result;
  }

  private SeriesSliceWrapper<T> fromEntity(Entity entity)
  {
    Long seq = (Long) entity.getProperty(SEQ);
    Integer maxSize = ((Number) entity.getProperty(MAX_SIZE)).intValue();
    Integer maxRes = ((Number)  entity.getProperty(MAX_RESOLUTION)).intValue();

    ArrayList<?> values = (ArrayList) entity.getProperty(VALUE);

    T[] sliceData = (T[]) Array.newInstance(serializer.getValueClass(), maxSize);

    for(int i = 0; i < maxSize; i++)
    {
      sliceData[i] = serializer.deserialize(values.get(i));
    }

    return new SeriesSliceWrapper<>(entity.getKey().getId(),
        seq, maxSize, maxRes, sliceData);
  }
}
