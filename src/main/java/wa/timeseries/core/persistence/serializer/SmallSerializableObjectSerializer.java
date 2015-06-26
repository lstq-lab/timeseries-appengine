package wa.timeseries.core.persistence.serializer;

import com.google.appengine.api.datastore.Blob;

import java.io.*;

public class SmallSerializableObjectSerializer<T extends Serializable>
    implements IValueSerializer<T>
{

  private final Class<T> clazz;

  public SmallSerializableObjectSerializer(Class<T> clazz)
  {
    this.clazz = clazz;
  }

  @Override public Blob serialize(T value)
  {
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = null;
    try
    {
      oos = new ObjectOutputStream(bos);
      oos.writeObject(value);
      oos.flush();
      return new Blob(bos.toByteArray());
    } catch (IOException e)
    {
      throw new RuntimeException(e);
    } finally
    {
      try
      {
        if (oos != null)
        {
          oos.close();
        }
        bos.close();
      } catch (IOException e)
      {
        throw new RuntimeException(e);
      }
    }
  }

  @Override public T deserialize(Object obj)
  {
    if (obj == null) return null;
    final Blob value = (Blob) obj;
    final ByteArrayInputStream bos = new ByteArrayInputStream(value.getBytes());
    ObjectInputStream ois = null;
    try
    {
      ois = new ObjectInputStream(bos);
      return (T) ois.readObject();
    } catch (IOException | ClassNotFoundException e)
    {
      throw new RuntimeException(e);
    } finally
    {
      try
      {
        if (ois != null)
        {
          ois.close();
        }
        bos.close();
      } catch (IOException e)
      {
        throw new RuntimeException(e);
      }
    }
  }

  @Override public Class<T> getValueClass()
  {
    return clazz;
  }
}
