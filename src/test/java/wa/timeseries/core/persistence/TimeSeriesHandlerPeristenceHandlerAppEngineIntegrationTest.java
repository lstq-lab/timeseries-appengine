package wa.timeseries.core.persistence;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import wa.timeseries.core.TimeSeries;
import wa.timeseries.core.TimeSeriesHandler;
import wa.timeseries.core.TimeSeriesID;
import wa.timeseries.core.persistence.serializer.IntegerSerializer;

import java.util.Iterator;

import static org.junit.Assert.*;

public class TimeSeriesHandlerPeristenceHandlerAppEngineIntegrationTest {

    private final LocalServiceTestHelper helper =
            new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());

    private TimeSeriesID TS_ID = new TimeSeriesID("ts","v-1");

    @Before
    public void setUp() {
        helper.setUp();
    }

    @After
    public void tearDown() {
        helper.tearDown();
    }

    @Test
    public void testGetUpdates() {
        TimeSeriesPeristenceHandlerAppEngine<Integer> handler =
                new TimeSeriesPeristenceHandlerAppEngine<>(
                        new IntegerSerializer());

        TimeSeriesHandler<Integer>
                ts = new TimeSeriesHandler<>(1, 0, handler);

        ts.add(TS_ID, 10, 0);
        ts.add(TS_ID, 11, 1);

        TimeSeriesID TS_ID_2 = new TimeSeriesID("ts","v-2");
        ts.add(TS_ID_2, 9, 0);

        Iterator<TimeSeries<Integer>> updates =
                ts.getUpdates(TS_ID_2.getFamily(), 10);
        assertTrue(updates.hasNext());
        TimeSeries<Integer> update = updates.next();
        assertFalse(updates.hasNext());

        assertEquals(TS_ID, update.getId());
        assertEquals(1, update.getLastValue().getValue().intValue());
        assertEquals(11, update.getLastValue().getDate());
    }

    @Test
    public void testPersistPrimitive() throws Exception {
        TimeSeriesPeristenceHandlerAppEngine<Integer> handler =
                new TimeSeriesPeristenceHandlerAppEngine<>(
                        new IntegerSerializer());
        TimeSeriesHandler<Integer> ts =
                new TimeSeriesHandler<>(100, 1, 0, handler);

        for (int i = 0; i < 1000; i++) {
            ts.add(TS_ID, i, i);
        }

        assertEquals(0, ts.get(TS_ID, 0).get().intValue());
        assertEquals(600, ts.get(TS_ID, 600).get().intValue());
        assertEquals(999, ts.get(TS_ID, 999).get().intValue());
        assertFalse(ts.get(TS_ID, 1000).isPresent());
    }
}