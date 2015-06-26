package wa.timeseries.core.persistence.serializer;


public class DoubleSerializer implements IValueSerializer<Double>
{

  @Override public Double serialize(Double value)
  {
    return value;
  }

  @Override public Double deserialize(Object value)
  {
    if (value == null) return null;
    return ((Number) value).doubleValue();
  }

  @Override public Class<Double> getValueClass()
  {
    return Double.class;
  }
}

