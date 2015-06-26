package wa.timeseries.core.persistence.serializer;


public class LongSerializer implements IValueSerializer<Long>
{

  @Override public Long serialize(Long value)
  {
    return value;
  }

  @Override public Long deserialize(Object value)
  {
    if (value == null) return null;
    return ((Number) value).longValue();
  }

  @Override public Class<Long> getValueClass()
  {
    return Long.class;
  }
}
