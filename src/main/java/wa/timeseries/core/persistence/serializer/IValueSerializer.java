package wa.timeseries.core.persistence.serializer;

import java.io.IOException;

/**
 * Serialize/deserialize an value to be persisted
 */
public interface IValueSerializer<T>
{

  Object serialize(T value) throws Exception;

  T deserialize(Object value) throws Exception;

  Class<T> getValueClass();

}
