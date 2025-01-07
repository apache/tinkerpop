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

import ch.qos.logback.classic.Level;
import io.netty.handler.codec.http.HttpResponseStatus;
import nl.altindag.log.LogCaptor;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.interceptor.PayloadSerializingInterceptor;
import org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin;
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for gremlin-driver configurations and settings.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinDriverIntegrateTest extends AbstractGremlinServerIntegrationTest {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(GremlinDriverIntegrateTest.class);

    private static LogCaptor logCaptor;
    private Level previousLogLevel;

    private static final RequestOptions groovyRequestOptions = RequestOptions.build().language("gremlin-groovy").create();

    @BeforeClass
    public static void setupLogCaptor() {
        logCaptor = LogCaptor.forRoot();
    }

    @AfterClass
    public static void tearDownAfterClass() {
        logCaptor.close();
    }

    @Before
    public void setupForEachTest() {
        logCaptor.clearLogs();
    }

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final String nameOfTest = name.getMethodName();

        switch (nameOfTest) {
            case "shouldInterceptRequests":
                settings.channelizer = HttpChannelizer.class.getName();
                break;
            case "shouldAliasTraversalSourceVariables":
                try {
                    final String p = Storage.toPath(TestHelper.generateTempFileFromResource(
                                                      GremlinDriverIntegrateTest.class,
                                                      "generate-shouldRebindTraversalSourceVariables.groovy", ""));
                    final Map<String,Object> m = new HashMap<>();
                    m.put("files", Collections.singletonList(p));
                    settings.scriptEngines.get("gremlin-groovy").plugins.put(ScriptFileGremlinPlugin.class.getName(), m);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                break;
            case "shouldFailWithBadClientSideSerialization":
                // add custom gryo config for Color
                final List<String> custom = Collections.singletonList(
                        Color.class.getName());
                settings.serializers.stream().filter(s -> s.className.contains("Gryo"))
                        .forEach(s -> s.config.put("custom", custom));
                break;
            case "shouldExecuteSessionlessScriptOnTransactionalGraph":
            case "shouldManageTransactionsInSession":
                useTinkerTransactionGraph(settings);
                break;
            case "shouldRequireAliasedGraphVariablesInStrictTransactionMode":
                settings.strictTransactionManagement = true;
                break;
            case "shouldAliasGraphVariablesInStrictTransactionMode":
                settings.strictTransactionManagement = true;
                useTinkerTransactionGraph(settings);
                break;
            case "shouldProcessSessionRequestsInOrderAfterTimeout":
                settings.evaluationTimeout = 250;
                settings.threadPoolWorker = 1;
                break;
            case "shouldProcessTraversalInterruption":
            case "shouldProcessEvalInterruption":
                settings.evaluationTimeout = 1500;
                break;
        }

        return settings;
    }

    @Test
    public void shouldInterceptRequests() throws Exception {
        final int requestsToMake = 32;
        final AtomicInteger httpRequests = new AtomicInteger(0);

        final Cluster cluster = TestClientFactory.build().
                addInterceptor("counter", r -> {
                    httpRequests.incrementAndGet();
                    return r;
                }).create();

        try {
            final Client client = cluster.connect();
            for (int ix = 0; ix < requestsToMake; ix++) {
                assertEquals(ix + 1, client.submit(String.format("g.inject(%d)", ix+1)).all().get().get(0).getInt());
            }
        } finally {
            cluster.close();
        }

        assertEquals(requestsToMake, httpRequests.get());
    }

    @Test
    public void shouldRunInterceptorsInOrder() throws Exception {
        AtomicReference<Object> body = new AtomicReference<>();
        final Cluster cluster = TestClientFactory.build().
                addInterceptor("first", r -> {
                    body.set(r.getBody());
                    r.setBody(null);
                    return r;
                }).
                addInterceptor("second", r -> {
                    r.setBody(body.get());
                    return r;
                }).create();

        try {
            final Client client = cluster.connect();
            assertEquals(2, client.submit("g.inject(2)").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldWorkWithGraphSONSerializer() throws Exception {
        final Cluster cluster = TestClientFactory.build(new PayloadSerializingInterceptor(new GraphSONMessageSerializerV4()))
                .serializer(Serializers.GRAPHSON_V4.simpleInstance()).create();

        try {
            final Client client = cluster.connect();
            final RequestOptions ro = RequestOptions.build()
                    .addG("gmodern")
                    .language("gremlin-lang")
                    .bulkResults(true)
                    .create();
            assertEquals(6, client.submit("g.V()", ro).all().get().size());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldWorkWithGraphSONRequestAndGraphBinaryResponse() throws Exception {
        final Cluster cluster = TestClientFactory.build(new PayloadSerializingInterceptor(new GraphSONMessageSerializerV4()))
                .serializer(Serializers.GRAPHBINARY_V4.simpleInstance()).create();

        try {
            final Client client = cluster.connect();
            assertEquals(3, client.submit("g.inject(3)").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldWorkWithGraphBinaryRequestAndGraphSONResponse() throws Exception {
        final Cluster cluster = TestClientFactory.build(new PayloadSerializingInterceptor(new GraphBinaryMessageSerializerV4()))
                .serializer(Serializers.GRAPHSON_V4.simpleInstance()).create();

        try {
            final Client client = cluster.connect();
            assertEquals(5, client.submit("g.inject(5)").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldInterceptRequestsWithHandshake() throws Exception {
        final int requestsToMake = 32;
        final AtomicInteger handshakeRequests = new AtomicInteger(0);

        final Cluster cluster = TestClientFactory.build().
                maxConnectionPoolSize(1).
                addInterceptor("counter", r -> {
                    handshakeRequests.incrementAndGet();
                    return r;
                }).create();

        try {
            final Client client = cluster.connect();
            for (int ix = 0; ix < requestsToMake; ix++) {
                final List<Result> result = client.submit(String.format("g.inject(%d)", ix+1)).all().get();
                assertEquals(ix + 1, result.get(0).getInt());
            }
        } finally {
            cluster.close();
        }

        assertEquals(requestsToMake, handshakeRequests.get());
    }

    @Ignore("Reading for streaming GraphSON is not supported")
    @Test
    public void shouldReportErrorWhenRequestCantBeSerialized() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V4).create();
        try {
            final Client client = cluster.connect().alias("g");

            try {
                final Map<String, Object> params = new HashMap<>();
                params.put("r", Color.RED);
                client.submit("g.inject(r)", params).all().get();
                fail("Should have thrown exception over bad serialization");
            } catch (Exception ex) {
                final Throwable inner = ExceptionHelper.getRootCause(ex);
                assertThat(inner, instanceOf(ResponseException.class));
                assertEquals(HttpResponseStatus.BAD_REQUEST, ((ResponseException) inner).getResponseStatusCode());
                assertTrue(ex.getMessage().contains("An error occurred during serialization of this request"));
            }

            // should not die completely just because we had a bad serialization error.  that kind of stuff happens
            // from time to time, especially in the console if you're just exploring.
            assertEquals(2, client.submit("g.inject(2)").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldProcessTraversalInterruption() {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("g.inject(1).sideEffect{Thread.sleep(5000)}", groovyRequestOptions).all().get();
            fail("Should have timed out");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, re.getResponseStatusCode());
            assertEquals("ServerTimeoutExceededException", re.getRemoteException());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldProcessEvalInterruption() {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("Thread.sleep(5000);'done'", groovyRequestOptions).all().get();
            fail("Should have timed out");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, re.getResponseStatusCode());
            assertEquals("ServerTimeoutExceededException", re.getRemoteException());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEventuallySucceedAfterChannelLevelError() {
        final Cluster cluster = TestClientFactory.build()
                .reconnectInterval(500)
                .maxResponseContentLength(32).create(); // Warning: compression can change the content length. Adjust as needed.
        final Client client = cluster.connect();

        try {
            try {
                client.submit("g.inject('x').repeat(concat('x')).times(512)").all().get();
                fail("Request should have failed because it exceeded the max content length allowed");
            } catch (Exception ex) {
                final Throwable root = ExceptionHelper.getRootCause(ex);
                assertThat(root.getMessage(), containsString("Response entity too large"));
            }

            assertEquals(2, client.submit("g.inject(2)").all().join().get(0).getInt());

        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEventuallySucceedAfterMuchFailure() {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            // tested independently to 10000 iterations but for speed, bumped back to 1000
            IntStream.range(0, 1000).forEach(i -> {
                try {
                    client.submit("1 + 9 9").all().join().get(0).getInt();
                    fail("Should not have gone through due to syntax error");
                } catch (Exception ex) {
                    final Throwable root = ExceptionHelper.getRootCause(ex);
                    assertThat(root, instanceOf(ResponseException.class));
                }
            });

            assertEquals(2, client.submit("g.inject(2)").all().join().get(0).getInt());

        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEventuallySucceedOnSameServerWithScript() throws Exception {
        stopServer();

        final Cluster cluster = TestClientFactory.build().validationRequest("g.inject()").create();
        final Client client = cluster.connect();

        try {
            client.submit("g.inject(2)").all().join().get(0).getInt();
            fail("Should not have gone through because the server is not running");
        } catch (Exception i) {
            assertThat(i, instanceOf(NoHostAvailableException.class));
            final Throwable root = ExceptionHelper.getRootCause(i);
            assertThat(root, instanceOf(ConnectException.class));
        }

        startServer();

        // default reconnect time is 1 second so wait some extra time to be sure it has time to try to bring it
        // back to life. usually this passes on the first attempt, but docker is sometimes slow and we get failures
        // waiting for Gremlin Server to pop back up
        for (int ix = 3; ix < 13; ix++) {
            TimeUnit.SECONDS.sleep(ix);
            try {
                final int result = client.submit("g.inject(2)").all().join().get(0).getInt();
                assertEquals(2, result);
                break;
            } catch (Exception ignored) {
                logger.warn("Attempt {} failed on shouldEventuallySucceedOnSameServerWithScript", ix);
            }
        }

        cluster.close();
    }

    @Test
    public void shouldEventuallySucceedWithRoundRobin() {
        final String noGremlinServer = "74.125.225.19";
        final Cluster cluster = TestClientFactory.build().addContactPoint(noGremlinServer).create();

        try {
            final Client client = cluster.connect();
            client.init();

            // the first host is dead on init.  request should succeed on localhost
            assertEquals(2, client.submit("g.inject(2)").all().join().get(0).getInt());
            assertEquals(2, client.submit("g.inject(2)").all().join().get(0).getInt());
            assertEquals(2, client.submit("g.inject(2)").all().join().get(0).getInt());
            assertEquals(2, client.submit("g.inject(2)").all().join().get(0).getInt());
            assertEquals(2, client.submit("g.inject(2)").all().join().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldHandleResultsOfAllSizes() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {

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
                    "    Vertex u = graph.vertices(ids.get(rand.nextInt(ids.size()))).next();\n" +
                    "    v.addEdge(\"linked\", u);\n" +
                    "    ids.add(u.id());\n" +
                    "    ids.add(v.id());\n" +
                    "}\n" +
                    "g.V()";

            final List<Integer> sizes = Arrays.asList(1, 10, 20, 50, 75, 100, 250, 500, 750, 1000, 5000, 10000);
            for (Integer size : sizes) {
                final Map<String, Object> params = new HashMap<>();
                params.put("size", size - 1);
                params.put("language", "gremlin-groovy");
                final ResultSet results = client.submit(script, params);

                assertEquals(size.intValue(), results.all().get().size());
            }
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailWithBadClientSideSerialization() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {

            final ResultSet results = client.submit("java.awt.Color.RED", groovyRequestOptions);

            try {
                results.all().join();
                fail("Should have thrown exception over bad serialization");
            } catch (Exception ex) {
                final Throwable inner = ExceptionHelper.getRootCause(ex);
                assertThat(inner, instanceOf(ResponseException.class));
                assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, ((ResponseException) inner).getResponseStatusCode());
            }

            // should not die completely just because we had a bad serialization error.  that kind of stuff happens
            // from time to time, especially in the console if you're just exploring.
            assertEquals(2, client.submit("g.inject(2)").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailWithScriptExecutionException() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();
        try {
            try {
                final ResultSet results = client.submit("g.inject(1).math('_/0')");
                results.all().join();
                fail("Should have thrown exception over division by zero");
            } catch (Exception ex) {
                final Throwable inner = ExceptionHelper.getRootCause(ex);
                assertTrue(inner instanceof ResponseException);
                assertThat(inner.getMessage(), containsString("Division by zero"));

                final ResponseException rex = (ResponseException) inner;
                assertEquals("ServerErrorException", rex.getRemoteException());
            }

            // should not die completely just because we had a bad serialization error.  that kind of stuff happens
            // from time to time, especially in the console if you're just exploring.
            assertEquals(2, client.submit("g.inject(2)").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldProcessRequestsOutOfOrder() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        try {
            final Client client = cluster.connect();

            final ResultSet rsFive = client.submit("Thread.sleep(5000);'five'", groovyRequestOptions);
            final ResultSet rsZero = client.submit("'zero'", groovyRequestOptions);

            final CompletableFuture<List<Result>> futureFive = rsFive.all();
            final CompletableFuture<List<Result>> futureZero = rsZero.all();

            final long start = System.nanoTime();
            assertFalse(futureFive.isDone());
            assertEquals("zero", futureZero.get().get(0).getString());

            logger.info("Eval of 'zero' complete: " + TimeUtil.millisSince(start));

            assertFalse(futureFive.isDone());
            assertEquals("five", futureFive.get(10, TimeUnit.SECONDS).get(0).getString());

            logger.info("Eval of 'five' complete: " + TimeUtil.millisSince(start));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldWaitForAllResultsToArrive() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        try {
            final Client client = cluster.connect();

            final AtomicInteger checked = new AtomicInteger(0);
            final ResultSet results = client.submit("g.inject(1,2,3,4,5,6,7,8,9)");
            while (!results.allItemsAvailable()) {
                assertTrue(results.getAvailableItemCount() < 10);
                checked.incrementAndGet();
                Thread.sleep(100);
            }

            assertTrue(checked.get() > 0);
            assertEquals(9, results.getAvailableItemCount());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldStream() {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            final ResultSet results = client.submit("g.inject(1,2,3,4,5,6,7,8,9)");
            final AtomicInteger counter = new AtomicInteger(0);
            results.stream().map(i -> i.get(Integer.class) * 2).forEach(i -> assertEquals(counter.incrementAndGet() * 2, Integer.parseInt(i.toString())));
            assertEquals(9, counter.get());
            assertThat(results.allItemsAvailable(), is(true));

            // cant stream it again
            assertThat(results.stream().iterator().hasNext(), is(false));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldIterate() {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            final ResultSet results = client.submit("g.inject(1,2,3,4,5,6,7,8,9)");
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
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldGetSomeThenSomeMore() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            final ResultSet results = client.submit("g.inject(1,2,3,4,5,6,7,8,9)");
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
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldGetOneThenSomeThenSomeMore() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            final ResultSet results = client.submit("g.inject(1,2,3,4,5,6,7,8,9)");
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
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAvoidDeadlockOnCallToResultSetDotAll() throws Exception {

        // This test arose from this issue: https://github.org/apache/tinkerpop/tinkerpop3/issues/515
        //
        // ResultSet.all returns a CompletableFuture that blocks on the worker pool until isExhausted returns false.
        // isExhausted in turn needs a thread on the worker pool to even return. So its totally possible to consume all
        // threads on the worker pool waiting for .all to finish such that you can't even get one to wait for
        // isExhausted to run.
        //
        // Note that all() doesn't work as described above anymore.  It waits for callback on readComplete rather
        // than blocking on isExhausted.
        final int workerPoolSizeForDriver = 2;

        // the number of requests 4 times the size of the worker pool as this originally did produce the problem
        // described above in the javadoc of the test (though an equivalent number also produced it), but this has
        // been tested to much higher multiples and passes.  note that the maxWaitForConnection setting is high so
        // that the client doesn't timeout waiting for an available connection. obviously this can also be fixed
        // by increasing the maxConnectionPoolSize.
        final int requests = workerPoolSizeForDriver * 4;
        final Cluster cluster = TestClientFactory.build()
                .workerPoolSize(workerPoolSizeForDriver)
                .maxWaitForConnection(300000)
                .create();
        try {
            final Client client = cluster.connect();

            final CountDownLatch latch = new CountDownLatch(requests);
            final AtomicReference[] refs = new AtomicReference[requests];
            IntStream.range(0, requests).forEach(ix -> {
                refs[ix] = new AtomicReference();
                client.submitAsync("Thread.sleep(5000);[1,2,3,4,5,6,7,8,9]", groovyRequestOptions).thenAccept(rs ->
                        rs.all().thenAccept(refs[ix]::set).thenRun(latch::countDown));
            });

            // countdown should have reached zero as results should have eventually been all returned and processed
            assertTrue(latch.await(30, TimeUnit.SECONDS));

            final List<Integer> expected = IntStream.range(1, 10).boxed().collect(Collectors.toList());
            IntStream.range(0, requests).forEach(r ->
                    assertTrue(expected.containsAll(((List<Result>) refs[r].get()).stream().map(resultItem -> new Integer(resultItem.getInt())).collect(Collectors.toList()))));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldCloseWithServerDown() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        try {
            cluster.connect().init();

            stopServer();
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldMarkHostDeadSinceServerIsDown() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        try {
            assertEquals(0, cluster.availableHosts().size());
            final Client client1 = cluster.connect().init();
            assertEquals(1, cluster.availableHosts().size());

            stopServer();

            // We create a new client here which will fail to initialize but the original client still has
            // host marked as connected. Since the second client failed during initialization, it has no way to
            // test if a host is indeed unreachable because it doesn't have any established connections. It will not add
            // the host to load balancer but it will also not remove it if it already exists there. Leave that
            // responsibility to a client that added it. In this case, let the second client perform it's own mechanism
            // to mark host as unavailable. The first client will discover that the host has failed either with next
            // keepAlive message or the next request, whichever is earlier. In this case, we will simulate the second
            // scenario by sending a new request on first client. The request would fail (since server is down) and
            // client should mark the host unavailable.
            cluster.connect().init();

            try {
                client1.submit("g.inject(2)").all().join();
                fail("Expecting an exception because the server is shut down.");
            } catch (Exception ex) {
                assertTrue(ex.getCause() instanceof TimeoutException);
                assertThat(ex.getMessage(), containsString("waiting for connection"));
            }

            assertEquals(0, cluster.availableHosts().size());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailWithBadServerSideSerialization() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();
        try {

            final ResultSet results = client.submit("TinkerGraph.open().variables()", groovyRequestOptions);

            try {
                results.all().join();
                fail();
            } catch (Exception ex) {
                final Throwable inner = ExceptionHelper.getRootCause(ex);
                assertTrue(inner instanceof ResponseException);
                assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, ((ResponseException) inner).getResponseStatusCode());
                assertEquals("ServerSerializationException", ((ResponseException) inner).getRemoteException());
            }

            // should not die completely just because we had a bad serialization error.  that kind of stuff happens
            // from time to time, especially in the console if you're just exploring.
            assertEquals(2, client.submit("g.inject(2)").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldWorkWithGraphBinaryV4Serialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHBINARY_V4).create();
        final Client client = cluster.connect();

        try {
            final List<Result> r = client.submit("TinkerFactory.createModern().traversal().V(1)", groovyRequestOptions).all().join();
            assertEquals(1, r.size());

            final Vertex v = r.get(0).get(DetachedVertex.class);
            assertEquals(1, v.id());
            assertEquals("person", v.label());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailClientSideWithTooLargeAResponse() {
        final Cluster cluster = TestClientFactory.build().maxResponseContentLength(1).create();
        final Client client = cluster.connect();

        try {
            final String fatty = IntStream.range(0, 100).mapToObj(String::valueOf).collect(Collectors.joining());
            client.submit(String.format("g.inject('%s')", fatty)).all().get();
            fail("Should throw an exception.");
        } catch (Exception re) {
            final Throwable root = ExceptionHelper.getRootCause(re);
            assertTrue(root.getMessage().contains("Response entity too large"));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldSucceedClientSideWithLargeResponseIfMaxResponseContentLengthZero() throws Exception {
        final Cluster cluster = TestClientFactory.build().maxResponseContentLength(0).create();
        final Client client = cluster.connect();

        try {
            final String fatty = IntStream.range(0, 10000).mapToObj(String::valueOf).collect(Collectors.joining());
            assertEquals(fatty, client.submit(String.format("g.inject('%s')", fatty)).all().get().get(0).getString());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldNotThrowNoSuchElementException() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            // this should return "nothing" - there should be no exception
            assertNull(client.submit("g.V().has('name','kadfjaldjfla')").one());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEvalInGremlinLangWithParameters() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            final RequestOptions ro = RequestOptions.build().language("gremlin-lang").
                    addParameter("x", 100).
                    addParameter("y", "test").
                    addParameter("z", true).create();
            final List<Result> l = client.submit("g.inject(x, y, z)", ro).all().get();
            assertEquals(100, l.get(0).getInt());
            assertEquals("test", l.get(1).getString());
            assertEquals(true, l.get(2).getBoolean());
        } finally {
            cluster.close();
        }
    }

    @Ignore("Server doesn't currently support transactions.")
    @Test
    public void shouldExecuteSessionlessScriptOnTransactionalGraph() throws Exception {

        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        // this line is important because it tests GraphTraversal which has a certain transactional path
        final Vertex vertexRequest1 = client.submit("g.addV().property(\"name\",\"stephen\")", groovyRequestOptions)
                .all().get().get(0).getVertex();

        final Vertex vertexRequest2 = client.submit("graph.vertices().next()", groovyRequestOptions)
                .all().get().get(0).getVertex();
        assertEquals(vertexRequest1.id(), vertexRequest2.id());

        // this line is important because it tests the other transactional path
        client.submit("graph.addVertex(\"name\",\"marko\")", groovyRequestOptions).all().get().get(0).getVertex();

        assertEquals(2, client.submit("g.V().count()", groovyRequestOptions).all().get().get(0).getLong());

        cluster.close();
    }

    @Test
    public void shouldNotHaveKnowledgeOfBindingsBetweenRequestsWhenSessionless() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client1 = cluster.connect();
        final Client client2 = cluster.connect();
        final Client client3 = cluster.connect();

        final ResultSet results11 = client1.submit("x = 1", groovyRequestOptions);
        final ResultSet results21 = client2.submit("x = 2", groovyRequestOptions);
        final ResultSet results31 = client3.submit("x = 3", groovyRequestOptions);
        assertEquals(1, results11.all().get().get(0).getInt());
        assertEquals(2, results21.all().get().get(0).getInt());
        assertEquals(3, results31.all().get().get(0).getInt());

        try {
            client1.submit("x", groovyRequestOptions).all().get();
            fail("The variable 'x' should not be present on the new request.");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            assertThat(root.getMessage(), containsString("No such property: x for class"));
        }

        try {
            client2.submit("x", groovyRequestOptions).all().get();
            fail("The variable 'x' should not be present on the new request.");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            assertThat(root.getMessage(), containsString("No such property: x for class"));
        }

        try {
            client3.submit("x", groovyRequestOptions).all().get();
            fail("The variable 'x' should not be present on the new request.");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            assertThat(root.getMessage(), containsString("No such property: x for class"));
        }

        cluster.close();
    }

    @Test
    public void shouldBeThreadSafeToUseOneClient() throws Exception {
        final Cluster cluster = TestClientFactory.build().workerPoolSize(2)
                .maxConnectionPoolSize(16).create();
        final Client client = cluster.connect();

        final Map<Integer, Integer> results = new ConcurrentHashMap<>();
        final List<Thread> threads = new ArrayList<>();
        for (int ix = 0; ix < 100; ix++) {
            final int otherNum = ix;
            final Thread t = new Thread(()->{
                try {
                    results.put(otherNum, client.submit(String.format("g.inject(%d)", 1000+otherNum)).all().get().get(0).getInt());
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }, name.getMethodName() + "-" + ix);

            t.start();
            threads.add(t);
        }

        threads.forEach(FunctionUtils.wrapConsumer(Thread::join));

        for (int ix = 0; ix < results.size(); ix++) {
            assertThat(results.containsKey(ix), is(true));
            assertEquals(1000 + ix, results.get(ix).intValue());
        }

        cluster.close();
    }

    @Ignore("used tx")
    @Test
    public void shouldRequireAliasedGraphVariablesInStrictTransactionMode() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("g.inject(2)").all().get();
            fail("Should have tossed an exception because strict mode is on and no aliasing was performed");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            final ResponseException re = (ResponseException) root;
            assertEquals(HttpResponseStatus.BAD_REQUEST, re.getResponseStatusCode());
        } finally {
            cluster.close();
        }
    }

    @Ignore("strict transactions not currently supported")
    @Test
    public void shouldAliasGraphVariablesInStrictTransactionMode() throws Exception {

        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("g.addVertex('name','stephen');",
                    groovyRequestOptions).all().get().get(0).getVertex();
            fail("Should have tossed an exception because \"g\" does not have the addVertex method under default config");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            final ResponseException re = (ResponseException) root;
            assertEquals(HttpResponseStatus.BAD_REQUEST, re.getResponseStatusCode());

            final Client rebound = cluster.connect().alias("graph");
            final Vertex v = rebound.submit("g.addVertex(T.label,'person')", groovyRequestOptions)
                    .all().get().get(0).getVertex();
            assertEquals("person", v.label());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAliasGraphVariables() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("g.addVertex(label,'person','name','stephen');",
                    groovyRequestOptions).all().get().get(0).getVertex();
            fail("Should have tossed an exception because \"g\" does not have the addVertex method under default config");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            final ResponseException re = (ResponseException) root;
            assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, re.getResponseStatusCode());

            final Client rebound = cluster.connect().alias("graph");
            final Vertex v = rebound.submit("g.addVertex(label,'person','name','jason')", groovyRequestOptions)
                    .all().get().get(0).getVertex();
            assertEquals("person", v.label());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAliasTraversalSourceVariables() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHBINARY_V4).create();
        final Client client = cluster.connect();
        try {
            try {
                client.submit("g.addV().property('name','stephen')").all().get().get(0).getVertex();
                fail("Should have tossed an exception because \"g\" is readonly in this context");
            } catch (Exception ex) {
                final Throwable root = ExceptionHelper.getRootCause(ex);
                assertThat(root, instanceOf(ResponseException.class));
                final ResponseException re = (ResponseException) root;
                assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, re.getResponseStatusCode());
            }

            final Client clientAliased = client.alias("g1");
            final String name = clientAliased.submit("g.addV().property('name','jason').values('name')").all().get().get(0).getString();
            assertEquals("jason", name);
        } finally {
            cluster.close();
        }
    }

    @Ignore("used sessions")
    @Test
    public void shouldCloseAllClientsOnCloseOfCluster() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client sessionlessOne = cluster.connect();
        final Client session = cluster.connect("session");
        final Client sessionlessTwo = cluster.connect();
        final Client sessionlessThree = cluster.connect();
        final Client sessionlessFour = cluster.connect();

        assertEquals(2, sessionlessOne.submit("g.inject(2)").all().get().get(0).getInt());
        assertEquals(2, session.submit("g.inject(2)").all().get().get(0).getInt());
        assertEquals(2, sessionlessTwo.submit("g.inject(2)").all().get().get(0).getInt());
        assertEquals(2, sessionlessThree.submit("g.inject(2)").all().get().get(0).getInt());
        // dont' send anything on the 4th client

        // close one of these Clients before the Cluster
        sessionlessThree.close();
        cluster.close();

        try {
            sessionlessOne.submit("g.inject(2)").all().get();
            fail("Should have tossed an exception because cluster was closed");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
            assertEquals("Client is closed", root.getMessage());
        }

        try {
            session.submit("g.inject(2)").all().get();
            fail("Should have tossed an exception because cluster was closed");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
            assertEquals("Client is closed", root.getMessage());
        }

        try {
            sessionlessTwo.submit("g.inject(2)").all().get();
            fail("Should have tossed an exception because cluster was closed");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
            assertEquals("Client is closed", root.getMessage());
        }

        try {
            sessionlessThree.submit("g.inject(2)").all().get();
            fail("Should have tossed an exception because cluster was closed");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
            assertEquals("Client is closed", root.getMessage());
        }

        try {
            sessionlessFour.submit("g.inject(2)").all().get();
            fail("Should have tossed an exception because cluster was closed");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
            assertEquals("Client is closed", root.getMessage());
        }

        // allow call to close() even though closed through cluster
        sessionlessOne.close();
        session.close();
        sessionlessTwo.close();

        cluster.close();
    }

    @Test
    public void shouldClusterReadFileFromResources() throws Exception {
        final Cluster cluster = Cluster.open(TestClientFactory.RESOURCE_PATH);
        assertNotNull(cluster);
        cluster.close();
    }

    /**
     * Tests to make sure that the stack trace contains an informative cause when the request times out because the
     * client was unable to get a connection and the pool is already maxed out.
     */
    @Test
    public void shouldReturnClearExceptionCauseWhenClientIsTooBusyAndConnectionPoolIsFull() throws InterruptedException {
        int maxSize = 1;
        final Cluster cluster = TestClientFactory.build()
                .maxConnectionPoolSize(1)
                .connectionSetupTimeoutMillis(100)
                .maxWaitForConnection(150)
                .create();

        final Client.ClusteredClient client = cluster.connect();

        for (int i = 0; i < 3; i++) {
            try {
                client.submitAsync("Thread.sleep(5000);", groovyRequestOptions);
            } catch (Exception e) {
                final Throwable root = ExceptionHelper.getRootCause(e);
                assertTrue(root instanceof TimeoutException);
                assertTrue(root.getMessage().contains(String.format(Client.TOO_MANY_IN_FLIGHT_REQUESTS, maxSize, maxSize)));
            }
        }

        cluster.close();
    }

    /**
     * Client created on an initially dead host should fail initially, and recover after the dead host has restarted
     * @param testClusterClient - boolean flag set to test clustered client if true and sessioned client if false.
     */
    private void testShouldFailOnInitiallyDeadHost(final boolean testClusterClient) throws Exception {
        logger.info("Stopping server.");
        this.stopServer();

        final Cluster cluster = TestClientFactory.build().create();
        final Client client = testClusterClient? cluster.connect() : cluster.connect("sessionClient");

        try {
            // try to re-issue a request now that the server is down
            logger.info("Verifying driver cannot connect to server.");
            client.submit("g").all().get(500, TimeUnit.MILLISECONDS);
            fail("Should throw an exception.");
        } catch (Exception re) {
            // Client would have no active connections to the host, hence it would encounter a timeout
            // trying to find an alive connection to the host.
            assertThat(re, instanceOf(NoHostAvailableException.class));

            try {
                client.submit("g.inject(2)").all().get(3000, TimeUnit.MILLISECONDS);
                fail("Should throw exception on the retry");
            } catch (RuntimeException re2) {
                assertThat(re2.getCause().getCause().getCause(), instanceOf(ConnectException.class));

            }

            //
            // should recover when the server comes back
            //

            // restart server
            logger.info("Restarting server.");
            this.startServer();

            // try a bunch of times to reconnect. on slower systems this may simply take longer...looking at you travis
            for (int ix = 1; ix < 11; ix++) {
                // the retry interval is 1 second, wait a bit longer
                TimeUnit.MILLISECONDS.sleep(1250);

                try {
                    logger.info(String.format("Connecting driver to server - attempt # %s. ", 1 + ix));
                    final List<Result> results = client.submit("g.inject(2)").all().get(3000, TimeUnit.MILLISECONDS);
                    assertEquals(1, results.size());
                    assertEquals(2, results.get(0).getInt());
                    logger.info("Connection successful.");
                    break;
                } catch (Exception ex) {
                    if (ix == 10)
                        fail("Should have eventually succeeded");
                }
            }
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailOnInitiallyDeadHostForClusterClient() throws Exception {
        testShouldFailOnInitiallyDeadHost(true);
    }
}
