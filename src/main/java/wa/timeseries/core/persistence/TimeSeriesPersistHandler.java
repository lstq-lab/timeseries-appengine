package wa.timeseries.core.persistence;

import com.google.appengine.api.datastore.*;
import wa.timeseries.core.*;
import wa.timeseries.core.persistence.serializer.IValueSerializer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

public class TimeSeriesPersistHandler<T> {

    private final Logger LOG = Logger.getLogger(TimeSeriesPersistHandler.class.getName());

    public static final String FAMILY = "family";

    public static final String ID = "id";

    public static final String LAST_VALUE = "lastValue";

    public static final String LAST_UPDATE = "lastUpdate";

    public static final String TS = "TS";

    private final IValueSerializer<T> serializer;

    private DatastoreService datastore;

    public TimeSeriesPersistHandler(IValueSerializer<T> serializer,
            final DatastoreService datastore) {
        this.serializer = serializer;
        this.datastore = datastore;
    }

    public Iterator<TimeSeries<T>> getUpdates(TimeSeriesConfiguration configuration, String family,
            long date) {

        LOG.info("Querying for Updates. Family:" + family + ", date: " + date);

        Query.Filter startFilter =
                new Query.FilterPredicate(FAMILY,
                        Query.FilterOperator.EQUAL,
                        family);
        Query.Filter endFilter =
                new Query.FilterPredicate(LAST_UPDATE,
                        Query.FilterOperator.GREATER_THAN_OR_EQUAL,
                        date);

        Query.Filter compFilter =
                Query.CompositeFilterOperator.and(startFilter, endFilter);

        Query q = new Query(TS).setFilter(compFilter);
        Iterable<Entity> iterable =
                datastore.prepare(q).asIterable();

        ArrayList<TimeSeries<T>> result = new ArrayList<>();

        //TODO return lazy iterator.
        for (Entity entity : iterable) {
            try {
                result.add(fromEntity(entity));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return result.iterator();
    }

    public TimeSeries<T> get(final TimeSeriesID tsId) {
        try {
            final Key tsKey = getTSKey(tsId);
            LOG.info("Getting TS: " + tsKey);
            Entity entity =
                    datastore.get(tsKey);
            return fromEntity(entity);
        } catch (EntityNotFoundException e) {
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Key persist(TimeSeries<T> timeSeries) {
        Entity entity = new Entity(getTSKey(timeSeries.getId()));

        entity.setProperty(FAMILY, timeSeries.getId().getFamily());
        entity.setProperty(ID, timeSeries.getId().getId());
        try {
            entity.setProperty(LAST_VALUE, serializer.serialize(
                    timeSeries.getLastValue().getValue()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        entity.setProperty(LAST_UPDATE, timeSeries.getLastValue().getDate());

        return datastore.put(entity);
    }

    private TimeSeries<T> fromEntity(Entity entity) throws Exception {

        final String family = (String) entity.getProperty(FAMILY);
        final String id = (String) entity.getProperty(ID);
        final long date = ((Number) entity.getProperty(LAST_UPDATE)).longValue();
        final Object rawValue = entity.getProperty(LAST_VALUE);

        final T value;
        try {

            value = serializer.deserialize(rawValue);
        } catch (Exception e) {
            LOG.severe("Error deserializing value. Deserializer: " + serializer + ", rawValue: " + rawValue);
            throw e;
        }

        return new TimeSeries<>(new TimeSeriesID(family, id),
                new DateValue<>(date, value));
    }

    private Key getTSKey(TimeSeriesID tsId) {
        return KeyFactory.createKey(TS, tsId.toString());
    }
}
