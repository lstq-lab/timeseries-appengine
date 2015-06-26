package wa.timeseries.core.persistence;

import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import wa.timeseries.core.SeriesSlice;
import wa.timeseries.core.persistence.serializer.DoubleSerializer;
import wa.timeseries.core.persistence.serializer.SmallSerializableObjectSerializer;

import java.io.Serializable;
import java.util.List;

import static org.junit.Assert.*;

public class SeriesSlicePersisterHandlerTest
{

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(
          new LocalDatastoreServiceTestConfig()
              .setApplyAllHighRepJobPolicy()
              .setNoIndexAutoGen(false));

  @Before
  public void setUp() {
    helper.setUp();
  }

  @After
  public void tearDown() {
    helper.tearDown();
  }


  @Test
  public void testFetchSlices() throws Exception
  {

  }

  @Test
  public void testPersistPrimitive() throws Exception
  {
    SeriesSlicePersisterHandler<Double> handler =
        new SeriesSlicePersisterHandler<>(new DoubleSerializer());

    Double[] data = new Double[10];
    data[0] = 0.0;
    data[5] = 5.0;
    data[9] = 10.0;

    SeriesSlice<Double> slice = new SeriesSlice<>(0, 10, 1, data);

    Key key = handler.persist(slice);

    assertNotNull(key);
    assertNotNull(DatastoreServiceFactory.getDatastoreService().get(key));

    List<SeriesSlice<Double>> list = handler.query(0, 1);

    assertEquals(1, list.size());

    SeriesSlice<Double> retrievedSlice = list.get(0);

    assertEquals(slice.getSeq(), retrievedSlice.getSeq());
    assertEquals(slice.getMaxSize(), retrievedSlice.getMaxSize());
    assertEquals(slice.getMaxResolution(), retrievedSlice.getMaxResolution());

    assertArrayEquals(slice.getRawSliceDataCopy(),
        retrievedSlice.getRawSliceDataCopy());
  }

  @Test
  public void testPersistObject() throws Exception
  {
    SeriesSlicePersisterHandler<TSTestValue> handler =
        new SeriesSlicePersisterHandler<>(new SmallSerializableObjectSerializer(
            TSTestValue.class));

    TSTestValue[] data = new TSTestValue[10];
    data[0] = new TSTestValue(0, 0);
    data[5] = new TSTestValue(5, 5);
    data[9] = new TSTestValue(9, 9);

    SeriesSlice<TSTestValue> slice = new SeriesSlice<>(0, 10, 1, data);

    Key key = handler.persist(slice);

    assertNotNull(key);
    assertNotNull(DatastoreServiceFactory.getDatastoreService().get(key));

    List<SeriesSlice<TSTestValue>> list = handler.query(0, 1);

    assertEquals(1, list.size());

    SeriesSlice<TSTestValue> retrievedSlice = list.get(0);

    assertEquals(slice.getSeq(), retrievedSlice.getSeq());
    assertEquals(slice.getMaxSize(), retrievedSlice.getMaxSize());
    assertEquals(slice.getMaxResolution(), retrievedSlice.getMaxResolution());

    int value1 = slice.getRawSliceDataCopy()[0].value1;
    int value11 = retrievedSlice.getRawSliceDataCopy()[0].value1;
    assertEquals(value1,
        value11);

    assertEquals(slice.getRawSliceDataCopy()[0].value2,
        retrievedSlice.getRawSliceDataCopy()[0].value2);

    assertEquals(slice.getRawSliceDataCopy()[0].value3,
        retrievedSlice.getRawSliceDataCopy()[0].value3);

    assertNull(retrievedSlice.getRawSliceDataCopy()[1]);

    assertEquals(slice.getRawSliceDataCopy()[5].value1,
        retrievedSlice.getRawSliceDataCopy()[5].value1);

    assertEquals(slice.getRawSliceDataCopy()[5].value2,
        retrievedSlice.getRawSliceDataCopy()[5].value2);

    assertEquals(slice.getRawSliceDataCopy()[5].value3,
        retrievedSlice.getRawSliceDataCopy()[5].value3);

    assertEquals(slice.getRawSliceDataCopy()[9].value1,
        retrievedSlice.getRawSliceDataCopy()[9].value1);

    assertEquals(slice.getRawSliceDataCopy()[9].value2,
        retrievedSlice.getRawSliceDataCopy()[9].value2);

    assertEquals(slice.getRawSliceDataCopy()[9].value3,
        retrievedSlice.getRawSliceDataCopy()[9].value3);


  }

  private static class TSTestValue implements Serializable
  {
    public int value1;
    public int value2;
    public String value3 = value1 + " -> " + value2;

    public TSTestValue(int value1, int value2)
    {
      this.value1 = value1;
      this.value2 = value2;
    }
  }

}