package wa.timeseries.core.persistence;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import wa.timeseries.core.TimeSeries;
import wa.timeseries.core.persistence.serializer.IntegerSerializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class TimeSeriesPeristenceHandlerAppEngineIntegrationTest
{

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());

  @Before
  public void setUp() {
    helper.setUp();
  }

  @After
  public void tearDown() {
    helper.tearDown();
  }

  @Test
  public void testPersistPrimitive() throws Exception
  {
    TimeSeriesPeristenceHandlerAppEngine<Integer> handler =
        new TimeSeriesPeristenceHandlerAppEngine<>(new IntegerSerializer());
    TimeSeries<Integer> ts = new TimeSeries<>(100, 1, 0, handler);

    for (int i = 0; i < 1000; i++)
    {
      ts.add(i, i);
    }

    assertEquals(0, ts.get(0).get().intValue());
    assertEquals(600, ts.get(600).get().intValue());
    assertEquals(999, ts.get(999).get().intValue());
    assertFalse(ts.get(1000).isPresent());
  }
}