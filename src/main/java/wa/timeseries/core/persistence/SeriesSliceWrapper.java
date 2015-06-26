package wa.timeseries.core.persistence;

import wa.timeseries.core.SeriesSlice;


public class SeriesSliceWrapper<T> extends SeriesSlice<T>
{

  private long id;

  public SeriesSliceWrapper(long id, long seq, int maxSize, int maxResolution, T[] rawData)
  {
    super(seq, maxSize, maxResolution, rawData);
    this.id = id;
  }

  public long getId()
  {
    return id;
  }
}
