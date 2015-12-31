package wa.timeseries.core.persistence;

import com.google.appengine.api.datastore.*;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import wa.timeseries.core.*;
import wa.timeseries.core.persistence.serializer.IValueSerializer;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Persister handler for Series Slices in AppEngine
 */
public class SeriesSlicePersistHandler<T> {

    public static final String MAX_SIZE = "maxSize";

    public static final String MAX_RESOLUTION = "maxResolution";

    public static final String LAST_UPDATE = "lastUpdate";

    private static final String VALUE = "value";

    public static final String TS_SLICE = "TSSlice";

    private final IValueSerializer<T> serializer;

    private DatastoreService datastore;

    public SeriesSlicePersistHandler(IValueSerializer<T> serializer, final DatastoreService datastore) {
        this.serializer = serializer;
        this.datastore = datastore;
    }

    /**
     * TODO Add Transaction support
     *
     * @param configuration
     * @param slice
     */
    public Key persist(TimeSeriesConfiguration configuration, TimeSeriesID tsId,
            final SeriesSlice<T> slice) {
        Entity entity = new Entity(getSliceId(configuration, tsId, slice.getSeq()));

        ArrayList<Object> values = new ArrayList<>();
        SeriesSlice<T>.SliceIterator iterator = slice.iterator();

        //The iterator is sorted. The serialized values will endup in the right
        //place in the values array.
        while (iterator.hasNext()) {
            OffsetValue<T> offsetValue = iterator.next();
            Object serialized = null;
            try {
                serialized = serializer.serialize(offsetValue.getValue());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            values.add(serialized);
        }

        entity.setUnindexedProperty(MAX_SIZE, slice.getMaxSize());
        entity.setUnindexedProperty(MAX_RESOLUTION, slice.getMaxResolution());
        entity.setProperty(LAST_UPDATE, new Date());
        entity.setUnindexedProperty(VALUE, values);

        return datastore.put(entity);
    }

    public List<SeriesSlice<T>> query(TimeSeriesConfiguration configuration,
            TimeSeriesID tsId, long seqStart, long seqEnd) {
        Filter startFilter =
                new FilterPredicate(Entity.KEY_RESERVED_PROPERTY,
                        FilterOperator.GREATER_THAN_OR_EQUAL,
                        getSliceId(configuration, tsId, seqStart));
        Filter endFilter =
                new FilterPredicate(Entity.KEY_RESERVED_PROPERTY,
                        FilterOperator.LESS_THAN_OR_EQUAL,
                        getSliceId(configuration, tsId, seqEnd));

        Filter compFilter =
                Query.CompositeFilterOperator.and(startFilter, endFilter);

        Query q = new Query(TS_SLICE).setFilter(compFilter);
        Iterable<Entity> iterable =
                datastore.prepare(q).asIterable();

        ArrayList<SeriesSlice<T>> result = new ArrayList<>();

        for (Entity entity : iterable) {
            result.add(fromEntity(entity));
        }

        return result;
    }

    private SeriesSliceWrapper<T> fromEntity(Entity entity) {
        final String[] keyComponents = entity.getKey().getName().split("-");
        final Long seq = Long.parseLong(keyComponents[keyComponents.length-1]);
        final Integer maxSize = ((Number) entity.getProperty(MAX_SIZE)).intValue();
        final Integer maxRes =
                ((Number) entity.getProperty(MAX_RESOLUTION)).intValue();

        ArrayList<?> values = (ArrayList) entity.getProperty(VALUE);

        T[] sliceData =
                (T[]) Array.newInstance(serializer.getValueClass(), maxSize);

        for (int i = 0; i < maxSize; i++) {
            try {
                sliceData[i] = serializer.deserialize(values.get(i));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return new SeriesSliceWrapper<>(entity.getKey().getId(),
                seq, maxSize, maxRes, sliceData);
    }

    private Key getSliceId(TimeSeriesConfiguration configuration, TimeSeriesID tsId, long sliceSeq) {
        return KeyFactory.createKey(TS_SLICE, tsId + "-" + configuration.getMaxResolution() + "-" + sliceSeq);
    }
}
