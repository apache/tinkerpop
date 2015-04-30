/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.JsonBuilderGryoSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import groovy.json.JsonBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Integration tests for gremlin-driver configurations and settings.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinDriverIntegrateTest extends AbstractGremlinServerIntegrationTest {

    @Rule
    public TestName name = new TestName();

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final String nameOfTest = name.getMethodName();

        // todo: how do we do transactional testing?
        /** removed neo4j - how do we test transactions now??
        switch (nameOfTest) {
            case "shouldExecuteScriptInSessionOnTransactionalGraph":
                deleteDirectory(new File("/tmp/neo4j"));
                settings.graphs.put("g", "conf/neo4j-empty.properties");
                break;
        }
         */

        return settings;
    }

    @Test
    public void shouldProcessRequestsOutOfOrder() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        final ResultSet rsFive = client.submit("Thread.sleep(5000);'five'");
        final ResultSet rsZero = client.submit("'zero'");

        final CompletableFuture<List<Result>> futureFive = rsFive.all();
        final CompletableFuture<List<Result>> futureZero = rsZero.all();

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

    /**
     * This test arose from this issue: https://github.org/apache/tinkerpop/tinkerpop3/issues/515
     * <p/>
     * ResultSet.all returns a CompleteableFuture that blocks on the worker pool until isExausted returns false.
     * isExausted in turn needs a thread on the worker pool to even return. So its totally possible to consume all
     * threads on the worker pool waiting for .all to finish such that you can't even get one to wait for
     * isExausted to run.
     */
    @Test
    public void shouldAvoidDeadlockOnCallToResultSetDotAll() throws Exception {
        final int workerPoolSizeForDriver = 2;

        // the number of requests 4 times the size of the worker pool as this originally did produce the problem
        // described above in the javadoc of the test (though an equivalent number also produced it), but this has
        // been tested to much higher multiples and passes.  note that the maxWaitForConnection setting is high so
        // that the client doesn't timeout waiting for an available connection. obviously this can also be fixed
        // by increasing the maxConnectionPoolSize.
        final int requests = workerPoolSizeForDriver * 4;
        final Cluster cluster = Cluster.build()
                .workerPoolSize(workerPoolSizeForDriver)
                .maxWaitForConnection(300000)
                .create();
        final Client client = cluster.connect();

        final CountDownLatch latch = new CountDownLatch(requests);
        final AtomicReference[] refs = new AtomicReference[requests];
        IntStream.range(0, requests).forEach(ix -> {
            refs[ix] = new AtomicReference();
            client.submitAsync("Thread.sleep(5000);[1,2,3,4,5,6,7,8,9]").thenAccept(rs ->
                rs.all().thenAccept(refs[ix]::set).thenRun(latch::countDown));
        });

        // countdown should have reached zero as results should have eventually been all returned and processed
        assertTrue(latch.await(20, TimeUnit.SECONDS));

        final List<Integer> expected = IntStream.range(1, 10).boxed().collect(Collectors.toList());
        IntStream.range(0, requests).forEach(r ->
            assertTrue(expected.containsAll(((List<Result>) refs[r].get()).stream().map(resultItem -> new Integer(resultItem.getInt())).collect(Collectors.toList()))));
    }

    @Test
    public void shouldHandleNullResult() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        final ResultSet results = client.submit("g.V().drop().iterate();null");
        assertNull(results.all().get().get(0).getObject());

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
    public void shouldMarkHostDeadSinceServerIsDown() throws Exception {
        stopServer();

        final Cluster cluster = Cluster.open();
        cluster.connect();

        assertEquals(0, cluster.availableHosts().size());

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
            results.all().join();
            fail();
        } catch (Exception ex) {
            final Throwable inner = ExceptionUtils.getRootCause(ex);
            assertTrue(inner instanceof ResponseException);
            assertEquals(ResponseStatusCode.SERVER_ERROR_SERIALIZATION, ((ResponseException) inner).getResponseStatusCode());
        }

        cluster.close();
    }

    @Test
    public void shouldSerializeToStringWhenRequested() throws Exception {
        final Map<String, Object> m = new HashMap<>();
        m.put("serializeResultToString", true);
        final GryoMessageSerializerV1d0 serializer = new GryoMessageSerializerV1d0();
        serializer.configure(m, null);

        final Cluster cluster = Cluster.build().serializer(serializer).create();
        final Client client = cluster.connect();

        final ResultSet resultSet = client.submit("TinkerFactory.createClassic()");
        final List<Result> results = resultSet.all().join();
        assertEquals(1, results.size());
        assertEquals("tinkergraph[vertices:6 edges:6]", results.get(0).getString());

        cluster.close();
    }

    @Test
    public void shouldDeserializeWithCustomClasses() throws Exception {
        final Map<String, Object> m = new HashMap<>();
        m.put("custom", Arrays.asList(String.format("%s;%s", JsonBuilder.class.getCanonicalName(), JsonBuilderGryoSerializer.class.getCanonicalName())));
        final GryoMessageSerializerV1d0 serializer = new GryoMessageSerializerV1d0();
        serializer.configure(m, null);

        final Cluster cluster = Cluster.build().serializer(serializer).create();
        final Client client = cluster.connect();

        final List<Result> json = client.submit("b = new JsonBuilder();b.people{person {fname 'stephen'\nlname 'mallette'}};b").all().join();
        assertEquals("{\"people\":{\"person\":{\"fname\":\"stephen\",\"lname\":\"mallette\"}}}", json.get(0).getString());
        cluster.close();
    }

    @Test
    public void shouldEventuallySucceedWithRoundRobin() throws Exception {
        final String noGremlinServer = "74.125.225.19";
        final Cluster cluster = Cluster.build(noGremlinServer).addContactPoint("localhost").create();
        final Client client = cluster.connect();

        // the first host is dead on init.  request should succeed on localhost
        assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        assertEquals(2, client.submit("1+1").all().join().get(0).getInt());

        cluster.close();
    }

    @Test
    public void shouldFailClientSideWithTooLargeAResponse() {
        final Cluster cluster = Cluster.build().maxContentLength(1).create();
        final Client client = cluster.connect();

        try {
            final String fatty = IntStream.range(0, 100).mapToObj(String::valueOf).collect(Collectors.joining());
            client.submit("'" + fatty + "'").all().get();
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

    @Test
    public void shouldExecuteScriptInSession() throws Exception {
        final Cluster cluster = Cluster.build().create();
        final Client client = cluster.connect(name.getMethodName());

        final ResultSet results1 = client.submit("x = [1,2,3,4,5,6,7,8,9]");
        final AtomicInteger counter = new AtomicInteger(0);
        results1.stream().map(i -> i.get(Integer.class) * 2).forEach(i -> assertEquals(counter.incrementAndGet() * 2, Integer.parseInt(i.toString())));

        final ResultSet results2 = client.submit("x[0]+1");
        assertEquals(2, results2.all().get().get(0).getInt());

        final ResultSet results3 = client.submit("x[1]+2");
        assertEquals(4, results3.all().get().get(0).getInt());

        cluster.close();
    }

    @Test
    public void shouldExecuteScriptInSessionAssumingDefaultedImports() throws Exception {
        final Cluster cluster = Cluster.build().create();
        final Client client = cluster.connect(name.getMethodName());

        final ResultSet results1 = client.submit("TinkerFactory.class.name");
        assertEquals(TinkerFactory.class.getName(), results1.all().get().get(0).getString());

        cluster.close();
    }

    @Test
    @org.junit.Ignore("Dropping neo4j prevents us from doing transactional tests")
    public void shouldExecuteScriptInSessionOnTransactionalGraph() throws Exception {
        final Cluster cluster = Cluster.build().create();
        final Client client = cluster.connect(name.getMethodName());

        final Vertex vertexBeforeTx = client.submit("v=graph.addVertex(\"name\",\"stephen\")").all().get().get(0).getVertex();
        assertEquals("stephen", vertexBeforeTx.values("name").next());

        final Vertex vertexFromV = client.submit("graph.vertices().next()").all().get().get(0).getVertex();
        assertEquals("stephen", vertexFromV.values("name").next());

        final Vertex vertexFromBinding = client.submit("v").all().get().get(0).getVertex();
        assertEquals("stephen", vertexFromBinding.values("name").next());

        final Vertex vertexAfterTx = client.submit("v.property(\"color\",\"blue\"); graph.tx().commit(); v").all().get().get(0).getVertex();
        assertEquals("stephen", vertexAfterTx.values("name").next());
        assertEquals("blue", vertexAfterTx.values("color").next());

        cluster.close();
    }

    @Test
    public void shouldExecuteScriptInSessionWithBindingsSavedOnServerBetweenRequests() throws Exception {
        final Cluster cluster = Cluster.build().create();
        final Client client = cluster.connect(name.getMethodName());

        final Map<String, Object> bindings1 = new HashMap<>();
        bindings1.put("a", 100);
        bindings1.put("b", 200);
        final ResultSet results1 = client.submit("x = a + b", bindings1);
        assertEquals(300, results1.one().getInt());

        final Map<String, Object> bindings2 = new HashMap<>();
        bindings2.put("b", 100);
        final ResultSet results2 = client.submit("x + b + a", bindings2);
        assertEquals(500, results2.one().getInt());

        final Map<String, Object> bindings3 = new HashMap<>();
        bindings3.put("x", 100);
        final ResultSet results3 = client.submit("x + b + a + 1", bindings3);
        assertEquals(301, results3.one().getInt());

        final Map<String, Object> bindings4 = new HashMap<>();
        final ResultSet results4 = client.submit("x + b + a + 1", bindings4);
        assertEquals(301, results4.one().getInt());

        cluster.close();
    }

    @Test
    public void shouldExecuteScriptsInMultipleSession() throws Exception {
        final Cluster cluster = Cluster.build().create();
        final Client client1 = cluster.connect(name.getMethodName() + "1");
        final Client client2 = cluster.connect(name.getMethodName() + "2");
        final Client client3 = cluster.connect(name.getMethodName() + "3");

        final ResultSet results11 = client1.submit("x = 1");
        final ResultSet results21 = client2.submit("x = 2");
        final ResultSet results31 = client3.submit("x = 3");
        assertEquals(1, results11.all().get().get(0).getInt());
        assertEquals(2, results21.all().get().get(0).getInt());
        assertEquals(3, results31.all().get().get(0).getInt());

        final ResultSet results12 = client1.submit("x + 100");
        final ResultSet results22 = client2.submit("x * 2");
        final ResultSet results32 = client3.submit("x * 10");
        assertEquals(101, results12.all().get().get(0).getInt());
        assertEquals(4, results22.all().get().get(0).getInt());
        assertEquals(30, results32.all().get().get(0).getInt());

        cluster.close();
    }
}
