package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Cluster;
import com.tinkerpop.gremlin.driver.Item;
import com.tinkerpop.gremlin.driver.ResultSet;
import com.tinkerpop.gremlin.util.TimeUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for gremlin-driver configurations and settings.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinDriverIntegrateTest extends AbstractGremlinServerIntegrationTest {

    @Test
    public void shouldProcessRequestsOutOfOrder() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        final ResultSet rsFive = client.submit("Thread.sleep(5000);'five'");
        final ResultSet rsZero = client.submit("'zero'");

        final CompletableFuture<List<Item>> futureFive = rsFive.all();
        final CompletableFuture<List<Item>> futureZero = rsZero.all();

        final long start = System.nanoTime();
        assertFalse(futureFive.isDone());
        assertEquals("zero", futureZero.get().get(0).getString());

        System.out.println("Eval of 'zero' complete: "  + TimeUtil.millisSince(start));

        assertFalse(futureFive.isDone());
        assertEquals("five", futureFive.get(10, TimeUnit.SECONDS).get(0).getString());

        System.out.println("Eval of 'five' complete: "  + TimeUtil.millisSince(start));
    }

    @Test
    public void shouldWaitForAllResultsToArrive() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        final AtomicInteger checked = new AtomicInteger(0);
        final ResultSet results = client.submit("[1,2,3,4,5,6,7,8,9]");
        while (!results.allItemsAvailable()) {
            assertTrue(results.getAvailableItemCount() < 10);
            checked.incrementAndGet();
            Thread.sleep(100);
        }

        assertTrue(checked.get() > 0);
        assertEquals(9, results.getAvailableItemCount());
        cluster.close();
    }

    @Test
    public void shouldStream() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        final ResultSet results = client.submit("[1,2,3,4,5,6,7,8,9]");
        final AtomicInteger counter = new AtomicInteger(0);
        results.stream().map(i -> i.get(Integer.class) * 2).forEach(i -> assertEquals(counter.incrementAndGet() * 2, Integer.parseInt(i.toString())));

        cluster.close();
    }

    @Test
    public void shouldCloseWithServerDown() throws Exception {
        final Cluster cluster = Cluster.open();
        cluster.connect();

        stopServer();

        cluster.close();
    }

    @Test
    public void shouldHandleRequestSentThatNeverReturns() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        final ResultSet results = client.submit("'should-not-ever-get-back-coz-we-killed-the-server'");

        stopServer();

        assertEquals(0, results.getAvailableItemCount());

        cluster.close();
    }
}
