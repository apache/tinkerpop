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
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.driver.Channelizer;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.JsonBuilderGryoSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.server.channel.NioChannelizer;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import groovy.json.JsonBuilder;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.StringStartsWith.startsWith;

/**
 * Integration tests for gremlin-driver configurations and settings.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinDriverIntegrateTest extends AbstractGremlinServerIntegrationTest {

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final String nameOfTest = name.getMethodName();

        switch (nameOfTest) {
            case "shouldAliasTraversalSourceVariables":
                try {
                    final String p = TestHelper.generateTempFileFromResource(
                            GremlinDriverIntegrateTest.class, "generate-shouldRebindTraversalSourceVariables.groovy", "").getAbsolutePath();
                    settings.scriptEngines.get("gremlin-groovy").scripts = Arrays.asList(p);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                break;
            case "shouldWorkOverNioTransport":
                settings.channelizer = NioChannelizer.class.getName();
                break;
            case "shouldFailWithBadClientSideSerialization":
                final List<String> custom = Arrays.asList(
                        JsonBuilder.class.getName() + ";" + JsonBuilderGryoSerializer.class.getName(),
                        java.awt.Color.class.getName());
                settings.serializers.stream().filter(s -> s.config.containsKey("custom"))
                        .findFirst().get().config.put("custom", custom);
                break;
            case "shouldExecuteScriptInSessionOnTransactionalGraph":
            case "shouldExecuteSessionlessScriptOnTransactionalGraph":
            case "shouldExecuteScriptInSessionOnTransactionalWithManualTransactionsGraph":
            case "shouldExecuteInSessionAndSessionlessWithoutOpeningTransaction":
                deleteDirectory(new File("/tmp/neo4j"));
                settings.graphs.put("graph", "conf/neo4j-empty.properties");
                break;
        }

        return settings;
    }

    @Test
    public void shouldHandleResultsOfAllSizes() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        final String script = "g.V().drop().iterate();\n" +
                "\n" +
                "List ids = new ArrayList();\n" +
                "\n" +
                "int ii = 0;\n" +
                "Vertex v = graph.addVertex();\n" +
                "v.property(\"ii\", ii);\n" +
                "v.property(\"sin\", Math.sin(ii));\n" +
                "ids.add(v.id());\n" +
                "\n" +
                "Random rand = new Random();\n" +
                "for (; ii < size; ii++) {\n" +
                "    v = graph.addVertex();\n" +
                "    v.property(\"ii\", ii);\n" +
                "    v.property(\"sin\", Math.sin(ii/5.0));\n" +
                "    Vertex u = g.V(ids.get(rand.nextInt(ids.size()))).next();\n" +
                "    v.addEdge(\"linked\", u);\n" +
                "    ids.add(u.id());\n" +
                "    ids.add(v.id());\n" +
                "}\n" +
                "g.V()";

        final List<Integer> sizes = Arrays.asList(1, 10, 20, 50, 75, 100, 250, 500, 750, 1000, 5000, 10000);
        for (Integer size : sizes) {
            final Map<String, Object> params = new HashMap<>();
            params.put("size", size - 1);
            final ResultSet results = client.submit(script, params);

            assertEquals(size.intValue(), results.all().get().size());
        }

        cluster.close();
    }

    @Test
    public void shouldFailWithBadClientSideSerialization() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        final ResultSet results = client.submit("java.awt.Color.RED");

        try {
            results.all().join();
            fail("Should have thrown exception over bad serialization");
        } catch (Exception ex) {
            final Throwable inner = ExceptionUtils.getRootCause(ex);
            assertTrue(inner instanceof RuntimeException);
            assertThat(inner.getMessage(), startsWith("Encountered unregistered class ID:"));
        }

        cluster.close();
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
    public void shouldProcessSessionRequestsInOrder() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect(name.getMethodName());

        final ResultSet rsFive = client.submit("Thread.sleep(5000);'five'");
        final ResultSet rsZero = client.submit("'zero'");

        final CompletableFuture<List<Result>> futureFive = rsFive.all();
        final CompletableFuture<List<Result>> futureZero = rsZero.all();

        final AtomicBoolean hit = new AtomicBoolean(false);
        while (!futureFive.isDone()) {
            // futureZero can't finish before futureFive - racy business here?
            assertThat(futureZero.isDone(), is(false));
            hit.set(true);
        }

        // should have entered the loop at least once and thus proven that futureZero didn't return ahead of
        // futureFive
        assertThat(hit.get(), is(true));

        assertEquals("zero", futureZero.get().get(0).getString());
        assertEquals("five", futureFive.get(10, TimeUnit.SECONDS).get(0).getString());
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
    public void shouldWorkOverNioTransport() throws Exception {
        final Cluster cluster = Cluster.build().channelizer(Channelizer.NioChannelizer.class.getName()).create();
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
        assertEquals(9, counter.get());
        assertThat(results.allItemsAvailable(), is(true));

        // cant stream it again
        assertThat(results.stream().iterator().hasNext(), is(false));

        cluster.close();
    }

    @Test
    public void shouldIterate() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        final ResultSet results = client.submit("[1,2,3,4,5,6,7,8,9]");
        final Iterator<Result> itty = results.iterator();
        final AtomicInteger counter = new AtomicInteger(0);
        while (itty.hasNext()) {
            counter.incrementAndGet();
            assertEquals(counter.get(), itty.next().getInt());
        }

        assertEquals(9, counter.get());
        assertThat(results.allItemsAvailable(), is(true));

        // can't stream it again
        assertThat(results.iterator().hasNext(), is(false));

        cluster.close();
    }

    @Test
    public void shouldGetSomeThenSomeMore() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        final ResultSet results = client.submit("[1,2,3,4,5,6,7,8,9]");
        final CompletableFuture<List<Result>> batch1 = results.some(5);
        final CompletableFuture<List<Result>> batch2 = results.some(5);
        final CompletableFuture<List<Result>> batchNothingLeft = results.some(5);

        assertEquals(5, batch1.get().size());
        assertEquals(1, batch1.get().get(0).getInt());
        assertEquals(2, batch1.get().get(1).getInt());
        assertEquals(3, batch1.get().get(2).getInt());
        assertEquals(4, batch1.get().get(3).getInt());
        assertEquals(5, batch1.get().get(4).getInt());

        assertEquals(4, batch2.get().size());
        assertEquals(6, batch2.get().get(0).getInt());
        assertEquals(7, batch2.get().get(1).getInt());
        assertEquals(8, batch2.get().get(2).getInt());
        assertEquals(9, batch2.get().get(3).getInt());

        assertEquals(0, batchNothingLeft.get().size());

        cluster.close();
    }

    @Test
    public void shouldGetOneThenSomeThenSomeMore() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        final ResultSet results = client.submit("[1,2,3,4,5,6,7,8,9]");
        final Result one = results.one();
        final CompletableFuture<List<Result>> batch1 = results.some(4);
        final CompletableFuture<List<Result>> batch2 = results.some(5);
        final CompletableFuture<List<Result>> batchNothingLeft = results.some(5);

        assertEquals(1, one.getInt());

        assertEquals(4, batch1.get().size());
        assertEquals(2, batch1.get().get(0).getInt());
        assertEquals(3, batch1.get().get(1).getInt());
        assertEquals(4, batch1.get().get(2).getInt());
        assertEquals(5, batch1.get().get(3).getInt());

        assertEquals(4, batch2.get().size());
        assertEquals(6, batch2.get().get(0).getInt());
        assertEquals(7, batch2.get().get(1).getInt());
        assertEquals(8, batch2.get().get(2).getInt());
        assertEquals(9, batch2.get().get(3).getInt());

        assertEquals(0, batchNothingLeft.get().size());

        cluster.close();
    }

    /**
     * This test arose from this issue: https://github.org/apache/tinkerpop/tinkerpop3/issues/515
     * <p/>
     * ResultSet.all returns a CompletableFuture that blocks on the worker pool until isExhausted returns false.
     * isExhausted in turn needs a thread on the worker pool to even return. So its totally possible to consume all
     * threads on the worker pool waiting for .all to finish such that you can't even get one to wait for
     * isExhausted to run.
     * <p/>
     * Note that all() doesn't work as described above anymore.  It waits for callback on readComplete rather
     * than blocking on isExhausted.
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
    public void shouldCloseWithServerDown() throws Exception {
        final Cluster cluster = Cluster.open();
        cluster.connect().init();

        stopServer();

        cluster.close();
    }

    @Test
    public void shouldMarkHostDeadSinceServerIsDown() throws Exception {
        final Cluster cluster = Cluster.open();
        assertEquals(0, cluster.availableHosts().size());
        cluster.connect().init();
        assertEquals(1, cluster.availableHosts().size());

        stopServer();

        cluster.connect().init();
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

        final ResultSet results = client.submit("TinkerGraph.open().variables()");

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
    public void shouldWorkWithGraphSONSerialization() throws Exception {
        final Cluster cluster = Cluster.build("localhost").serializer(Serializers.GRAPHSON_V1D0).create();
        final Client client = cluster.connect();

        final List<Result> r = client.submit("TinkerFactory.createModern().traversal().V(1)").all().join();
        assertEquals(1, r.size());

        final Map<String,Object> m = r.get(0).get(Map.class);
        assertEquals(4, m.size());
        assertEquals(1, m.get("id"));
        assertEquals("person", m.get("label"));
        assertEquals("vertex", m.get("type"));

        final Map<String,Object> properties = (Map<String,Object>) m.get("properties");
        assertEquals(2, properties.size());

        final List<Object> names = (List<Object>) properties.get("name");
        assertEquals(1, names.size());

        final Map<String,Object> nameProperties = (Map<String,Object>) names.get(0);
        assertEquals(2, nameProperties.size());
        assertEquals(0l, nameProperties.get("id"));
        assertEquals("marko", nameProperties.get("value"));

        final List<Object> ages = (List<Object>) properties.get("age");
        assertEquals(1, ages.size());

        final Map<String,Object> ageProperties = (Map<String,Object>) ages.get(0);
        assertEquals(2, ageProperties.size());
        assertEquals(1l, ageProperties.get("id"));
        assertEquals(29, ageProperties.get("value"));

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
            final Throwable root = ExceptionUtils.getRootCause(re);
            assertTrue(root.getMessage().equals("Max frame length of 1 has been exceeded."));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldExecuteScriptInSession() throws Exception {
        final Cluster cluster = Cluster.build().create();
        final Client client = cluster.connect(name.getMethodName());

        final ResultSet results1 = client.submit("x = [1,2,3,4,5,6,7,8,9]");
        assertEquals(9, results1.all().get().size());

        final ResultSet results2 = client.submit("x[0]+1");
        assertEquals(2, results2.all().get().get(0).getInt());

        final ResultSet results3 = client.submit("x[1]+2");
        assertEquals(4, results3.all().get().get(0).getInt());

        cluster.close();
    }

    @Test
    public void shouldCloseSession() throws Exception {
        final Cluster cluster = Cluster.build().create();
        final Client client = cluster.connect(name.getMethodName());

        final ResultSet results1 = client.submit("x = [1,2,3,4,5,6,7,8,9]");
        assertEquals(9, results1.all().get().size());
        final ResultSet results2 = client.submit("x[0]+1");
        assertEquals(2, results2.all().get().get(0).getInt());

        client.close();

        try {
            client.submit("x[0]+1");
            fail("Should have thrown an exception because the connection is closed");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(ConnectionException.class));
        }
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
    public void shouldExecuteScriptInSessionOnTransactionalGraph() throws Exception {
        assumeNeo4jIsPresent();

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
    public void shouldExecuteScriptInSessionOnTransactionalWithManualTransactionsGraph() throws Exception {
        assumeNeo4jIsPresent();

        final Cluster cluster = Cluster.build().create();
        final Client client = cluster.connect(name.getMethodName());
        final Client sessionlessClient = cluster.connect();
        client.submit("graph.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);null").all().get();
        client.submit("graph.tx().open()").all().get();

        final Vertex vertexBeforeTx = client.submit("v=graph.addVertex(\"name\",\"stephen\")").all().get().get(0).getVertex();
        assertEquals("stephen", vertexBeforeTx.values("name").next());

        final Vertex vertexFromV = client.submit("graph.vertices().next()").all().get().get(0).getVertex();
        assertEquals("stephen", vertexFromV.values("name").next());

        final Vertex vertexFromBinding = client.submit("v").all().get().get(0).getVertex();
        assertEquals("stephen", vertexFromBinding.values("name").next());

        client.submit("v.property(\"color\",\"blue\")").all().get();
        client.submit("graph.tx().commit()").all().get();
        
        // Run a sessionless request to change transaction.readWriteConsumer back to AUTO
        // The will make the next in session request fail if consumers aren't ThreadLocal
        sessionlessClient.submit("graph.vertices().next()").all().get();
        
        client.submit("graph.tx().open()").all().get();

        final Vertex vertexAfterTx = client.submit("graph.vertices().next()").all().get().get(0).getVertex();
        assertEquals("stephen", vertexAfterTx.values("name").next());
        assertEquals("blue", vertexAfterTx.values("color").next());

        client.submit("graph.tx().rollback()").all().get();

        cluster.close();
    }

    @Test
    public void shouldExecuteInSessionAndSessionlessWithoutOpeningTransaction() throws Exception {
        assumeNeo4jIsPresent();
        
        final Cluster cluster = Cluster.build().create();
        final Client sessionClient = cluster.connect(name.getMethodName());
        final Client sessionlessClient = cluster.connect();
        
        //open transaction in session, then add vertex and commit
        sessionClient.submit("graph.tx().open()").all().get();
        final Vertex vertexBeforeTx = sessionClient.submit("v=graph.addVertex(\"name\",\"stephen\")").all().get().get(0).getVertex();
        assertEquals("stephen", vertexBeforeTx.values("name").next());
        sessionClient.submit("graph.tx().commit()").all().get();
        
        // check that session transaction is closed
        final boolean isOpen = sessionClient.submit("graph.tx().isOpen()").all().get().get(0).getBoolean();
        assertTrue("Transaction should be closed", !isOpen);
        
        //run a sessionless read
        sessionlessClient.submit("graph.traversal().V()").all().get();
        
        // check that session transaction is still closed
        final boolean isOpenAfterSessionless = sessionClient.submit("graph.tx().isOpen()").all().get().get(0).getBoolean();
        assertTrue("Transaction should stil be closed", !isOpenAfterSessionless);
        
    }

    @Test
    public void shouldExecuteSessionlessScriptOnTransactionalGraph() throws Exception {
        assumeNeo4jIsPresent();

        final Cluster cluster = Cluster.build().create();
        final Client client = cluster.connect();

        final Vertex vertexRequest1 = client.submit("graph.addVertex(\"name\",\"stephen\")").all().get().get(0).getVertex();
        assertEquals("stephen", vertexRequest1.values("name").next());

        final Vertex vertexRequest2 = client.submit("graph.vertices().next()").all().get().get(0).getVertex();
        assertEquals("stephen", vertexRequest2.values("name").next());

        final Vertex vertexRequest3 = client.submit("graph.addVertex(\"name\",\"marko\")").all().get().get(0).getVertex();
        assertEquals("marko", vertexRequest3.values("name").next());

        assertEquals(2, client.submit("g.V().count()").all().get().get(0).getLong());

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

    @Test
    public void shouldBeThreadSafeToUseOneClient() throws Exception {
        final Cluster cluster = Cluster.build().create();
        final Client client = cluster.connect();

        final Map<Integer, Integer> results = new ConcurrentHashMap<>();
        final List<Thread> threads = new ArrayList<>();
        for (int ix = 0; ix < 100; ix++) {
            final int otherNum = ix;
            final Thread t = new Thread(()->{
                try {
                    results.put(otherNum, client.submit("1000+" + otherNum).all().get().get(0).getInt());
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }, name.getMethodName() + "-" + ix);

            t.start();
            threads.add(t);
        }

        threads.forEach(FunctionUtils.wrapConsumer(Thread::join));

        for (int ix = 0; ix < results.size(); ix++) {
            assertEquals(1000 + ix, results.get(ix).intValue());
        }
    }

    @Test
    public void shouldAliasGraphVariables() throws Exception {
        final Cluster cluster = Cluster.build().create();
        final Client client = cluster.connect();

        try {
            client.submit("g.addVertex('name','stephen');").all().get().get(0).getVertex();
            fail("Should have tossed an exception because \"g\" does not have the addVertex method under default config");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            final ResponseException re = (ResponseException) root;
            assertEquals(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION, re.getResponseStatusCode());
        }

        // keep the testing here until "rebind" is completely removed
        final Client reboundLegacy = cluster.connect().rebind("graph");
        final Vertex vLegacy = reboundLegacy.submit("g.addVertex('name','stephen')").all().get().get(0).getVertex();
        assertEquals("stephen", vLegacy.value("name"));

        final Client rebound = cluster.connect().alias("graph");
        final Vertex v = rebound.submit("g.addVertex('name','jason')").all().get().get(0).getVertex();
        assertEquals("jason", v.value("name"));

        cluster.close();
    }

    @Test
    public void shouldAliasTraversalSourceVariables() throws Exception {
        final Cluster cluster = Cluster.build().create();
        final Client client = cluster.connect();

        try {
            client.submit("g.addV('name','stephen')").all().get().get(0).getVertex();
            fail("Should have tossed an exception because \"g\" is readonly in this context");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            final ResponseException re = (ResponseException) root;
            assertEquals(ResponseStatusCode.SERVER_ERROR, re.getResponseStatusCode());
        }

        // keep the testing here until "rebind" is completely removed
        final Client clientLegacy = client.rebind("g1");
        final Vertex vLegacy = clientLegacy.submit("g.addV('name','stephen')").all().get().get(0).getVertex();
        assertEquals("stephen", vLegacy.value("name"));

        final Client clientAliased = client.alias("g1");
        final Vertex v = clientAliased.submit("g.addV('name','jason')").all().get().get(0).getVertex();
        assertEquals("jason", v.value("name"));

        cluster.close();
    }
}
