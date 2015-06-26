package wa.timeseries.core.persistence;

import wa.timeseries.core.SeriesSlice;
import wa.timeseries.core.persistence.serializer.IValueSerializer;

import java.util.Iterator;

public class TimeSeriesPeristenceHandlerAppEngine<T> implements
    TimeSeriesPersistenceHandler<T>
{
  private final SeriesSlicePersisterHandler<T> persisterHandler;

  public TimeSeriesPeristenceHandlerAppEngine(
      IValueSerializer<T> serializer)
  {
    this.persisterHandler = new SeriesSlicePersisterHandler<>(serializer);
  }

  @Override public Iterator<SeriesSlice<T>> fetchSlices(long seqStart,
      long seqEnd)
  {
    return persisterHandler.query(seqStart, seqEnd).iterator();
  }

  @Override public void persist(SeriesSlice<T> slice)
  {
    persisterHandler.persist(slice);
  }
}
