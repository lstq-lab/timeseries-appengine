package wa.timeseries.core.persistence.serializer;


public class IntegerSerializer implements IValueSerializer<Integer>
{

  @Override public Integer serialize(Integer value)
  {
    return value;
  }

  @Override public Integer deserialize(Object value)
  {
    if (value == null) return null;
    return ((Number) value).intValue();
  }

  @Override public Class<Integer> getValueClass()
  {
    return Integer.class;
  }
}
