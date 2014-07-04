package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Cluster;
import com.tinkerpop.gremlin.driver.Item;
import com.tinkerpop.gremlin.driver.ResultSet;
import com.tinkerpop.gremlin.driver.exception.ResponseException;
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.driver.ser.JsonBuilderKryoSerializer;
import com.tinkerpop.gremlin.driver.ser.KryoMessageSerializerV1d0;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.util.TimeUtil;
import com.tinkerpop.gremlin.util.function.SFunction;
import groovy.json.JsonBuilder;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for gremlin-driver configurations and settings.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinDriverIntegrateTest extends AbstractGremlinServerIntegrationTest {

    @Rule
    public TestName name = new TestName();

    public static class RemoteTraversal implements SFunction<Graph, Traversal> {

        // todo: consider how to parameterize traversals - is it best done client side during construction or through bindings as a SBiFunction<Graph, Map, Traversal>

        public Traversal apply(final Graph g) {
            return g.V().out().range(0, 9);
        }
    }

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldSendTraversal":
                settings.scriptEngines.get("gremlin-groovy").scripts.add("scripts/generate-sample.groovy");
                break;
        }

        return settings;
    }

    @Test
    public void shouldSendTraversal() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        final List<Item> results = client.submit(new RemoteTraversal()).all().get();
        assertEquals(10, results.size());
        cluster.close();
    }

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

        System.out.println("Eval of 'zero' complete: " + TimeUtil.millisSince(start));

        assertFalse(futureFive.isDone());
        assertEquals("five", futureFive.get(10, TimeUnit.SECONDS).get(0).getString());

        System.out.println("Eval of 'five' complete: " + TimeUtil.millisSince(start));
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

    @Test
    public void shouldFailWithBadServerSideSerialization() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        final ResultSet results = client.submit("TinkerFactory.createClassic()");

        try {
            final CompletableFuture<List<Item>> all = results.all();
            all.join();
            fail();
        } catch (Exception ex) {
            final Throwable inner = ex.getCause().getCause();
            assertTrue(inner instanceof ResponseException);
            assertEquals(ResultCode.SERVER_ERROR_SERIALIZATION, ((ResponseException) inner).getResultCode());
        }

        cluster.close();
    }

    @Test
    public void shouldSerializeToStringWhenRequested() throws Exception {
        final Map<String, Object> m = new HashMap<>();
        m.put("serializeResultToString", true);
        final KryoMessageSerializerV1d0 serializer = new KryoMessageSerializerV1d0();
        serializer.configure(m);

        final Cluster cluster = Cluster.create().serializer(serializer).build();
        final Client client = cluster.connect();

        final ResultSet results = client.submit("TinkerFactory.createClassic()");
        final List<Item> items = results.all().join();
        assertEquals(1, items.size());
        assertEquals("tinkergraph[vertices:6 edges:6]", items.get(0).getString());

        cluster.close();
    }

    @Test
    public void shouldDeserializeWithCustomClasses() throws Exception {
        final Map<String, Object> m = new HashMap<>();
        m.put("custom", Arrays.asList(String.format("%s;%s", JsonBuilder.class.getCanonicalName(), JsonBuilderKryoSerializer.class.getCanonicalName())));
        final KryoMessageSerializerV1d0 serializer = new KryoMessageSerializerV1d0();
        serializer.configure(m);

        final Cluster cluster = Cluster.create().serializer(serializer).build();
        final Client client = cluster.connect();

        final List<Item> json = client.submit("b = new JsonBuilder();b.people{person {fname 'stephen'\nlname 'mallette'}};b").all().join();
        assertEquals("{\"people\":{\"person\":{\"fname\":\"stephen\",\"lname\":\"mallette\"}}}", json.get(0).getString());
        cluster.close();
    }

    @Test
    public void shouldEventuallySucceedWithRoundRobin() throws Exception {
        // todo: when we have a config on borrowed connection timeout, set it low here to make the test go faster.
        final String noGremlinServer = "74.125.225.19";
        final Cluster cluster = Cluster.create(noGremlinServer).addContactPoint("localhost").build();
        final Client client = cluster.connect();

        try {
            // this first attempt will fail because it's sending stuff to a host it can't connect to
            client.submit("1+1").all().join();
            fail();
        } catch (RuntimeException re) {
            assertTrue(re.getCause().getCause() instanceof TimeoutException);
        }

        // ensure that connection to server is good - that host should be marked "dead" and remaining
        // requests should succeed
        assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        assertEquals(2, client.submit("1+1").all().join().get(0).getInt());

        cluster.close();
    }

    @Test
    @Ignore
    public void shouldFailClientSideWithTooLargeAResponse() {
        // todo: maybe a netty issue to look into - test won't pass

        final Cluster cluster = Cluster.create().maxContentLength(1).build();
        final Client client = cluster.connect();

        try {
            final String fatty = IntStream.range(0, 100).mapToObj(String::valueOf).collect(Collectors.joining());
            client.submitAsync("'" + fatty + "'").get().all();
            fail("Should throw an exception.");
        } catch (Exception re) {
            // can't seem to catch the server side exception - as the channel is basically closed on this error
            // can only detect a closed channel and react to that.  in some ways this is a good general piece of
            // code to have in place, but kinda stinky when you want something specific about why all went bad
            assertTrue(re.getCause().getMessage().equals("Error while processing results from channel - check client and server logs for more information"));
        } finally {
            cluster.close();
        }
    }
}
