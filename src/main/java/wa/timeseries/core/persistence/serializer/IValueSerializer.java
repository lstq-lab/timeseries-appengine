package wa.timeseries.core.persistence.serializer;

/**
 * Serialize/deserialize an value to be persisted
 */
public interface IValueSerializer<T>
{

  Object serialize(T value);

  T deserialize(Object value);

  Class<T> getValueClass();

}
