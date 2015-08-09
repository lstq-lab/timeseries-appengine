package wa.timeseries.core.persistence;

import com.google.appengine.api.datastore.DatastoreServiceFactory;
import wa.timeseries.core.SeriesSlice;
import wa.timeseries.core.TimeSeries;
import wa.timeseries.core.TimeSeriesConfiguration;
import wa.timeseries.core.TimeSeriesID;
import wa.timeseries.core.persistence.serializer.IValueSerializer;

import java.util.Iterator;

public class TimeSeriesPeristenceHandlerAppEngine<T> implements
    TimeSeriesPersistenceHandler<T> {

    private final SeriesSlicePersistHandler<T> slicePersistHandler;

    private final TimeSeriesPersistHandler<T> tsPersistHandler;

    public TimeSeriesPeristenceHandlerAppEngine(
            IValueSerializer<T> serializer) {
        this.tsPersistHandler = new TimeSeriesPersistHandler<>(serializer,
                DatastoreServiceFactory.getDatastoreService());
        this.slicePersistHandler = new SeriesSlicePersistHandler<>(serializer,
                DatastoreServiceFactory.getDatastoreService());
    }

    @Override public Iterator<SeriesSlice<T>> fetchSlices(
            TimeSeriesConfiguration configuration, TimeSeriesID tsId,
            long seqStart,
            long seqEnd) {
        return slicePersistHandler.query(configuration, tsId, seqStart, seqEnd)
                .iterator();
    }

    @Override public void persist(TimeSeriesConfiguration configuration,
            TimeSeriesID tsId, SeriesSlice<T> slice) {
        slicePersistHandler.persist(configuration, tsId, slice);
    }

    @Override public Iterator<TimeSeries<T>> getUpdates(
            TimeSeriesConfiguration configuration, String family, long date) {
        return tsPersistHandler.getUpdates(configuration, family, date);
    }

    @Override public TimeSeries<T> get(TimeSeriesID tsId) {
        return tsPersistHandler.get(tsId);
    }

    @Override public void persist(TimeSeries timeSeries) {
        tsPersistHandler.persist(timeSeries);
    }
}
