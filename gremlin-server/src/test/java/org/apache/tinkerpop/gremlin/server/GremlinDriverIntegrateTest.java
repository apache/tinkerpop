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
import ch.qos.logback.classic.Logger;
import io.netty.handler.codec.http.HttpResponseStatus;
import nl.altindag.log.LogCaptor;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.driver.Channelizer;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.handler.GremlinResponseHandler;
import org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin;
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;
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
        if (name.getMethodName().equals("shouldKeepAliveForWebSockets") ||
                name.getMethodName().equals("shouldKeepAliveForWebSocketsWithNoInFlightRequests")) {
            final Logger webSocketClientHandlerLogger = (Logger) LoggerFactory.getLogger(GremlinResponseHandler.class);
            previousLogLevel = webSocketClientHandlerLogger.getLevel();
            webSocketClientHandlerLogger.setLevel(Level.DEBUG);
        }

        logCaptor.clearLogs();
    }

    @After
    public void afterEachTest() {
        if (name.getMethodName().equals("shouldKeepAliveForWebSockets") ||
                name.getMethodName().equals("shouldKeepAliveForWebSocketsWithNoInFlightRequests")) {
            final Logger webSocketClientHandlerLogger = (Logger) LoggerFactory.getLogger(GremlinResponseHandler.class);
            webSocketClientHandlerLogger.setLevel(previousLogLevel);
        }
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
            case "shouldAliasTraversalSourceVariablesInSession":
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
            case "shouldExecuteScriptInSessionOnTransactionalGraph":
            case "shouldExecuteSessionlessScriptOnTransactionalGraph":
            case "shouldExecuteScriptInSessionOnTransactionalWithManualTransactionsGraph":
            case "shouldExecuteInSessionAndSessionlessWithoutOpeningTransaction":
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
                channelizer(Channelizer.HttpChannelizer.class).
                requestInterceptor(r -> {
                    httpRequests.incrementAndGet();
                    return r;
                }).create();

        try {
            final Client client = cluster.connect();
            for (int ix = 0; ix < requestsToMake; ix++) {
                assertEquals(ix + 1, client.submit(ix + "+1").all().get().get(0).getInt());
            }
        } finally {
            cluster.close();
        }

        assertEquals(requestsToMake, httpRequests.get());
    }

    @Test
    public void shouldInterceptRequestsWithHandshake() throws Exception {
        final int requestsToMake = 32;
        final AtomicInteger websocketHandshakeRequests = new AtomicInteger(0);

        final Cluster cluster = TestClientFactory.build().
                minConnectionPoolSize(1).maxConnectionPoolSize(1).
                handshakeInterceptor(r -> {
            websocketHandshakeRequests.incrementAndGet();
            return r;
        }).create();

        try {
            final Client client = cluster.connect();
            for (int ix = 0; ix < requestsToMake; ix++) {
                assertEquals(ix + 1, client.submit(ix + "+1").all().get().get(0).getInt());
            }
        } finally {
            cluster.close();
        }

        assertEquals(1, websocketHandshakeRequests.get());
    }

    @Test
    public void shouldReportErrorWhenRequestCantBeSerialized() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V3).create();
        try {
            final Client client = cluster.connect().alias("g");

            try {
                final Map<String, Object> params = new HashMap<>();
                params.put("r", Color.RED);
                client.submit("r", params).all().get();
                fail("Should have thrown exception over bad serialization");
            } catch (Exception ex) {
                final Throwable inner = ExceptionHelper.getRootCause(ex);
                assertThat(inner, instanceOf(ResponseException.class));
                assertEquals(HttpResponseStatus.BAD_REQUEST, ((ResponseException) inner).getResponseStatusCode());
                assertTrue(ex.getMessage().contains("An error occurred during serialization of this request"));
            }

            // should not die completely just because we had a bad serialization error.  that kind of stuff happens
            // from time to time, especially in the console if you're just exploring.
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldProcessTraversalInterruption() {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("g.inject(1).sideEffect{Thread.sleep(5000)}").all().get();
            fail("Should have timed out");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, re.getResponseStatusCode());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldProcessEvalInterruption() {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("Thread.sleep(5000);'done'").all().get();
            fail("Should have timed out");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, re.getResponseStatusCode());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldKeepAliveForWebSockets() throws Exception {
        // keep the connection pool size at 1 to remove the possibility of lots of connections trying to ping which will
        // complicate the assertion logic
        final Cluster cluster = TestClientFactory.build().
                minConnectionPoolSize(1).
                maxConnectionPoolSize(1).
                keepAliveInterval(1002).create();
        try {
            final Client client = cluster.connect();

            // fire up lots of requests so as to schedule/deschedule lots of ping jobs
            for (int ix = 0; ix < 500; ix++) {
                assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            }

            // don't send any messages for a bit so that the driver pings in the background
            Thread.sleep(3000);

            // make sure no bonus messages sorta fire off once we get back to sending requests
            for (int ix = 0; ix < 500; ix++) {
                assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            }

            // there really shouldn't be more than 3 of these sent. should definitely be at least one though
            final long messages = logCaptor.getLogs().stream().filter(m -> m.contains("Sending ping frame to the server")).count();
            assertThat(messages, allOf(greaterThan(0L), lessThanOrEqualTo(3L)));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldKeepAliveForWebSocketsWithNoInFlightRequests() throws Exception {
        // keep the connection pool size at 1 to remove the possibility of lots of connections trying to ping which will
        // complicate the assertion logic
        final Cluster cluster = TestClientFactory.build().
                minConnectionPoolSize(1).
                maxConnectionPoolSize(1).
                keepAliveInterval(1002).create();
        try {
            final Client client = cluster.connect();

            // forcefully initialize the client to mimic a scenario when client has some active connection with no
            // in flight requests on them.
            client.init();

            // don't send any messages for a bit so that the driver pings in the background
            Thread.sleep(3000);

            // make sure no bonus messages sorta fire off once we get back to sending requests
            for (int ix = 0; ix < 500; ix++) {
                assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            }

            // there really shouldn't be more than 3 of these sent. should definitely be at least one though
            final long messages = logCaptor.getLogs().stream().filter(m -> m.contains("Sending ping frame to the server")).count();
            assertThat(messages, allOf(greaterThan(0L), lessThanOrEqualTo(3L)));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEventuallySucceedAfterChannelLevelError() throws Exception {
        final Cluster cluster = TestClientFactory.build()
                .reconnectInterval(500)
                .maxContentLength(64).create();
        final Client client = cluster.connect();

        try {
            try {
                client.submit("def x = '';(0..<1024).each{x = x + '$it'};x").all().get();
                fail("Request should have failed because it exceeded the max content length allowed");
            } catch (Exception ex) {
                final Throwable root = ExceptionHelper.getRootCause(ex);
                assertThat(root.getMessage(), containsString("Max frame length of 64 has been exceeded."));
            }

            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());

        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEventuallySucceedAfterMuchFailure() throws Exception {
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

            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());

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
            client.submit("1+1").all().join().get(0).getInt();
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
                final int result = client.submit("1+1").all().join().get(0).getInt();
                assertEquals(2, result);
                break;
            } catch (Exception ignored) {
                logger.warn("Attempt {} failed on shouldEventuallySucceedOnSameServerWithScript", ix);
            }
        }

        cluster.close();
    }

    @Test
    public void shouldEventuallySucceedWithRoundRobin() throws Exception {
        final String noGremlinServer = "74.125.225.19";
        final Cluster cluster = TestClientFactory.build().addContactPoint(noGremlinServer).create();

        try {
            final Client client = cluster.connect();
            client.init();

            // the first host is dead on init.  request should succeed on localhost
            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
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

            final ResultSet results = client.submit("java.awt.Color.RED");

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
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
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
                final ResultSet results = client.submit("1/0");
                results.all().join();
                fail("Should have thrown exception over division by zero");
            } catch (Exception ex) {
                final Throwable inner = ExceptionHelper.getRootCause(ex);
                assertTrue(inner instanceof ResponseException);
                assertThat(inner.getMessage(), endsWith("Division by zero"));

                final ResponseException rex = (ResponseException) inner;
                assertEquals("java.lang.ArithmeticException", rex.getRemoteExceptionHierarchy().get().get(0));
                assertEquals(1, rex.getRemoteExceptionHierarchy().get().size());
                assertThat(rex.getRemoteStackTrace().get(), containsString("Division by zero"));
            }

            // should not die completely just because we had a bad serialization error.  that kind of stuff happens
            // from time to time, especially in the console if you're just exploring.
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldProcessRequestsOutOfOrder() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        try {
            final Client client = cluster.connect();

            final ResultSet rsFive = client.submit("Thread.sleep(5000);'five'");
            final ResultSet rsZero = client.submit("'zero'");

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
            final ResultSet results = client.submit("[1,2,3,4,5,6,7,8,9]");
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
            final ResultSet results = client.submit("[1,2,3,4,5,6,7,8,9]");
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
    public void shouldIterate() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
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
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldGetSomeThenSomeMore() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
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
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldGetOneThenSomeThenSomeMore() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
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
                client.submitAsync("Thread.sleep(5000);[1,2,3,4,5,6,7,8,9]").thenAccept(rs ->
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
                client1.submit("1+1").all().join();
                fail("Expecting an exception because the server is shut down.");
            } catch (Exception ex) {
                // ignore the exception
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

            final ResultSet results = client.submit("TinkerGraph.open().variables()");

            try {
                results.all().join();
                fail();
            } catch (Exception ex) {
                final Throwable inner = ExceptionHelper.getRootCause(ex);
                assertTrue(inner instanceof ResponseException);
                assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, ((ResponseException) inner).getResponseStatusCode());
            }

            // should not die completely just because we had a bad serialization error.  that kind of stuff happens
            // from time to time, especially in the console if you're just exploring.
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldSerializeToStringWhenRequestedGraphBinaryV1() throws Exception {
        final Map<String, Object> m = new HashMap<>();
        m.put("serializeResultToString", true);
        final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1();
        serializer.configure(m, null);

        final Cluster cluster = TestClientFactory.build().serializer(serializer).create();
        final Client client = cluster.connect();

        final ResultSet resultSet = client.submit("TinkerFactory.createClassic()");
        final List<Result> results = resultSet.all().join();
        assertEquals(1, results.size());
        assertEquals("tinkergraph[vertices:6 edges:6]", results.get(0).getString());

        cluster.close();
    }

    @Test
    public void shouldWorkWithGraphSONV1Serialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V1_UNTYPED).create();
        final Client client = cluster.connect();

        try {
            final List<Result> r = client.submit("TinkerFactory.createModern().traversal().V(1)").all().join();
            assertEquals(1, r.size());

            final Map<String, Object> m = r.get(0).get(Map.class);
            assertEquals(4, m.size());
            assertEquals(1, m.get("id"));
            assertEquals("person", m.get("label"));
            assertEquals("vertex", m.get("type"));

            final Map<String, Object> properties = (Map<String, Object>) m.get("properties");
            assertEquals(2, properties.size());

            final List<Object> names = (List<Object>) properties.get("name");
            assertEquals(1, names.size());

            final Map<String, Object> nameProperties = (Map<String, Object>) names.get(0);
            assertEquals(2, nameProperties.size());
            assertEquals(0, nameProperties.get("id"));
            assertEquals("marko", nameProperties.get("value"));

            final List<Object> ages = (List<Object>) properties.get("age");
            assertEquals(1, ages.size());

            final Map<String, Object> ageProperties = (Map<String, Object>) ages.get(0);
            assertEquals(2, ageProperties.size());
            assertEquals(1, ageProperties.get("id"));
            assertEquals(29, ageProperties.get("value"));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldWorkWithGraphSONV2Serialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V2).create();
        final Client client = cluster.connect();

        try {
            final List<Result> r = client.submit("TinkerFactory.createModern().traversal().V(1)").all().join();
            assertEquals(1, r.size());

            final Vertex v = r.get(0).get(DetachedVertex.class);
            assertEquals(1, v.id());
            assertEquals("person", v.label());

            assertEquals(2, IteratorUtils.count(v.properties()));
            assertEquals("marko", v.value("name"));
            assertEquals(29, Integer.parseInt(v.value("age").toString()));
        } finally {
            cluster.close();
        }
    }

//    @Test
//    public void shouldWorkWithGraphSONExtendedV2Serialization() {
//        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V2).create();
//        final Client client = cluster.connect();
//
//        try {
//            final List<Result> r = client.submit("java.time.Instant.EPOCH").all().join();
//            assertEquals(1, r.size());
//
//            final Instant then = r.get(0).get(Instant.class);
//            assertEquals(Instant.EPOCH, then);
//        } finally {
//            cluster.close();
//        }
//    }
//
//    @Test
//    public void shouldWorkWithGraphSONV3Serialization() throws Exception {
//        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V3).create();
//        final Client client = cluster.connect();
//
//        try {
//            final List<Result> r = client.submit("TinkerFactory.createModern().traversal().V(1)").all().join();
//            assertEquals(1, r.size());
//
//            final Vertex v = r.get(0).get(DetachedVertex.class);
//            assertEquals(1, v.id());
//            assertEquals("person", v.label());
//
//            assertEquals(2, IteratorUtils.count(v.properties()));
//            assertEquals("marko", v.value("name"));
//            assertEquals(29, Integer.parseInt(v.value("age").toString()));
//        } finally {
//            cluster.close();
//        }
//    }
//
//    @Test
//    public void shouldWorkWithGraphSONExtendedV3Serialization() throws Exception {
//        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V3).create();
//        final Client client = cluster.connect();
//
//        try {
//            final List<Result> r = client.submit("java.time.Instant.EPOCH").all().join();
//            assertEquals(1, r.size());
//
//            final Instant then = r.get(0).get(Instant.class);
//            assertEquals(Instant.EPOCH, then);
//        } finally {
//            cluster.close();
//        }
//    }
//
//    @Test
//    public void shouldWorkWithGraphBinaryV1Serialization() throws Exception {
//        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHBINARY_V1).create();
//        final Client client = cluster.connect();
//
//        try {
//            final List<Result> r = client.submit("TinkerFactory.createModern().traversal().V(1)").all().join();
//            assertEquals(1, r.size());
//
//            final Vertex v = r.get(0).get(DetachedVertex.class);
//            assertEquals(1, v.id());
//            assertEquals("person", v.label());
//        } finally {
//            cluster.close();
//        }
//    }

    @Test
    public void shouldFailClientSideWithTooLargeAResponse() {
        final Cluster cluster = TestClientFactory.build().maxContentLength(1).create();
        final Client client = cluster.connect();

        try {
            final String fatty = IntStream.range(0, 100).mapToObj(String::valueOf).collect(Collectors.joining());
            client.submit("'" + fatty + "'").all().get();
            fail("Should throw an exception.");
        } catch (Exception re) {
            final Throwable root = ExceptionHelper.getRootCause(re);
            assertTrue(root.getMessage().equals("Max frame length of 1 has been exceeded."));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldReturnNiceMessageFromOpSelector() {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            final Map m = new HashMap<>();
            m.put(null, "a null key will force a throw of OpProcessorException in message validation");
            client.submit("1+1", m).all().get();
            fail("Should throw an exception.");
        } catch (Exception re) {
            final Throwable root = ExceptionHelper.getRootCause(re);
            assertEquals("The [eval] message is using one or more invalid binding keys - they must be of type String and cannot be null", root.getMessage());
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
    public void shouldEvalInGremlinLang() {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            final RequestOptions ro = RequestOptions.build().language("gremlin-lang").create();
            assertEquals(111, client.submit("g.inject(111)", ro).one().getInt());
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

    @Test
    public void shouldExecuteSessionlessScriptOnTransactionalGraph() throws Exception {

        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        // this line is important because it tests GraphTraversal which has a certain transactional path
        final Vertex vertexRequest1 = client.submit("g.addV().property(\"name\",\"stephen\")").all().get().get(0).getVertex();

        final Vertex vertexRequest2 = client.submit("graph.vertices().next()").all().get().get(0).getVertex();
        assertEquals(vertexRequest1.id(), vertexRequest2.id());

        // this line is important because it tests the other transactional path
        client.submit("graph.addVertex(\"name\",\"marko\")").all().get().get(0).getVertex();

        assertEquals(2, client.submit("g.V().count()").all().get().get(0).getLong());

        cluster.close();
    }

    @Test
    public void shouldExecuteScriptInSessionWithBindingsSavedOnServerBetweenRequests() throws Exception {
        final Cluster cluster = TestClientFactory.open();
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
        final Cluster cluster = TestClientFactory.open();
        try {
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
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldNotHaveKnowledgeOfBindingsBetweenRequestsWhenSessionless() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client1 = cluster.connect();
        final Client client2 = cluster.connect();
        final Client client3 = cluster.connect();

        final ResultSet results11 = client1.submit("x = 1");
        final ResultSet results21 = client2.submit("x = 2");
        final ResultSet results31 = client3.submit("x = 3");
        assertEquals(1, results11.all().get().get(0).getInt());
        assertEquals(2, results21.all().get().get(0).getInt());
        assertEquals(3, results31.all().get().get(0).getInt());

        try {
            client1.submit("x").all().get();
            fail("The variable 'x' should not be present on the new request.");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            assertThat(root.getMessage(), containsString("No such property: x for class"));
        }

        try {
            client2.submit("x").all().get();
            fail("The variable 'x' should not be present on the new request.");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            assertThat(root.getMessage(), containsString("No such property: x for class"));
        }

        try {
            client3.submit("x").all().get();
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
                .maxConnectionPoolSize(16)
                .minConnectionPoolSize(8).create();
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
            assertThat(results.containsKey(ix), is(true));
            assertEquals(1000 + ix, results.get(ix).intValue());
        }

        cluster.close();
    }

    @Test
    public void shouldRequireAliasedGraphVariablesInStrictTransactionMode() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all().get();
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

    @Test
    public void shouldAliasGraphVariablesInStrictTransactionMode() throws Exception {

        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("g.addVertex('name','stephen');").all().get().get(0).getVertex();
            fail("Should have tossed an exception because \"g\" does not have the addVertex method under default config");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            final ResponseException re = (ResponseException) root;
            assertEquals(HttpResponseStatus.BAD_REQUEST, re.getResponseStatusCode());

            final Client rebound = cluster.connect().alias("graph");
            final Vertex v = rebound.submit("g.addVertex(T.label,'person')").all().get().get(0).getVertex();
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
            client.submit("g.addVertex(label,'person','name','stephen');").all().get().get(0).getVertex();
            fail("Should have tossed an exception because \"g\" does not have the addVertex method under default config");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            final ResponseException re = (ResponseException) root;
            assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, re.getResponseStatusCode());

            final Client rebound = cluster.connect().alias("graph");
            final Vertex v = rebound.submit("g.addVertex(label,'person','name','jason')").all().get().get(0).getVertex();
            assertEquals("person", v.label());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAliasTraversalSourceVariables() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHBINARY_V1).create();
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

    @Test
    public void shouldAliasGraphVariablesInSession() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHBINARY_V1).create();
        final Client client = cluster.connect(name.getMethodName());

        try {
            client.submit("g.addVertex('name','stephen');").all().get().get(0).getVertex();
            fail("Should have tossed an exception because \"g\" does not have the addVertex method under default config");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            final ResponseException re = (ResponseException) root;
            assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, re.getResponseStatusCode());
            client.close();
        }

        try {
            final Client aliased = cluster.connect(name.getMethodName()).alias("graph");
            assertEquals("jason", aliased.submit("n='jason'").all().get().get(0).getString());
            final String name = aliased.submit("g.addVertex('name',n).values('name')").all().get().get(0).getString();
            assertEquals("jason", name);
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAliasTraversalSourceVariablesInSession() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHBINARY_V1).create();
        final Client client = cluster.connect(name.getMethodName());

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
        assertEquals("jason", clientAliased.submit("n='jason'").all().get().get(0).getString());
        final String name = clientAliased.submit("g.addV().property('name',n).values('name')").all().get().get(0).getString();
        assertEquals("jason", name);

        cluster.close();
    }

    @Test
    public void shouldCloseAllClientsOnCloseOfCluster() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client sessionlessOne = cluster.connect();
        final Client session = cluster.connect("session");
        final Client sessionlessTwo = cluster.connect();
        final Client sessionlessThree = cluster.connect();
        final Client sessionlessFour = cluster.connect();

        assertEquals(2, sessionlessOne.submit("1+1").all().get().get(0).getInt());
        assertEquals(2, session.submit("1+1").all().get().get(0).getInt());
        assertEquals(2, sessionlessTwo.submit("1+1").all().get().get(0).getInt());
        assertEquals(2, sessionlessThree.submit("1+1").all().get().get(0).getInt());
        // dont' send anything on the 4th client

        // close one of these Clients before the Cluster
        sessionlessThree.close();
        cluster.close();

        try {
            sessionlessOne.submit("1+1").all().get();
            fail("Should have tossed an exception because cluster was closed");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
            assertEquals("Client is closed", root.getMessage());
        }

        try {
            session.submit("1+1").all().get();
            fail("Should have tossed an exception because cluster was closed");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
            assertEquals("Client is closed", root.getMessage());
        }

        try {
            sessionlessTwo.submit("1+1").all().get();
            fail("Should have tossed an exception because cluster was closed");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
            assertEquals("Client is closed", root.getMessage());
        }

        try {
            sessionlessThree.submit("1+1").all().get();
            fail("Should have tossed an exception because cluster was closed");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
            assertEquals("Client is closed", root.getMessage());
        }

        try {
            sessionlessFour.submit("1+1").all().get();
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

//    TODO: should probably remove and deprecate this old user-agent.
//    @Test
//    public void shouldSendUserAgent() throws Exception {
//        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V3).create();
//        final Client client = Mockito.spy(cluster.connect().alias("g"));
//        client.submit("", RequestOptions.build().userAgent("test").create()).all().get();
//        cluster.close();
//
//        final ArgumentCaptor<RequestMessage> requestMessageCaptor = ArgumentCaptor.forClass(RequestMessage.class);
//        verify(client).submitAsync(requestMessageCaptor.capture());
//        final RequestMessage requestMessage = requestMessageCaptor.getValue();
//        assertEquals("test", requestMessage.getArgs().get(Tokens.ARGS_USER_AGENT));
//    }

//    TODO: should probably remove and deprecate this old user-agent.
//    @Test
//    public void shouldSendUserAgentBytecode() {
//        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V3).create();
//        final Client client = Mockito.spy(cluster.connect().alias("g"));
//        Mockito.when(client.alias("g")).thenReturn(client);
//        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(client));
//        g.with(Tokens.ARGS_USER_AGENT, "test").V().iterate();
//        cluster.close();
//
//        final ArgumentCaptor<RequestOptions> requestOptionsCaptor = ArgumentCaptor.forClass(RequestOptions.class);
//        verify(client).submitAsync(Mockito.any(Bytecode.class), requestOptionsCaptor.capture());
//        final RequestOptions requestOptions = requestOptionsCaptor.getValue();
//        assertEquals("test", requestOptions.getUserAgent().get());
//
//        final ArgumentCaptor<RequestMessage> requestMessageCaptor = ArgumentCaptor.forClass(RequestMessage.class);
//        verify(client).submitAsync(requestMessageCaptor.capture());
//        final RequestMessage requestMessage = requestMessageCaptor.getValue();
//        assertEquals("test", requestMessage.getArgs().getOrDefault(Tokens.ARGS_USER_AGENT, null));
//    }

    @Test
    public void shouldClusterReadFileFromResources() throws Exception {
        final Cluster cluster = Cluster.open(TestClientFactory.RESOURCE_PATH);
        assertNotNull(cluster);
        cluster.close();
    }

    @Test
    public void shouldNotHangWhenSameRequestIdIsUsed() throws Exception {
        final Cluster cluster = TestClientFactory.build().maxConnectionPoolSize(1).minConnectionPoolSize(1).create();
        final Client client = cluster.connect();
        final UUID requestId = UUID.randomUUID();

        final Future<ResultSet> result1 = client.submitAsync("Thread.sleep(2000);100",
                RequestOptions.build().overrideRequestId(requestId).create());

        // wait for some business to happen on the server
        Thread.sleep(100);
        try {
            // re-use the id and fail
            client.submit("1+1+97", RequestOptions.build().overrideRequestId(requestId).create());
            fail("Request should not have been sent due to duplicate id");
        } catch(Exception ex) {
            // should get a rejection here
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root.getMessage(), startsWith("There is already a request pending with an id of:"));
            assertEquals(100, result1.get().one().getInt());
        } finally {
            cluster.close();
        }
    }

    /**
     * Tests to make sure that the stack trace contains an informative cause when the request times out because the
     * client was unable to get a connection and the pool is already maxed out.
     */
    @Test
    public void shouldReturnClearExceptionCauseWhenClientIsTooBusyAndConnectionPoolIsFull() throws InterruptedException {
        final Cluster cluster = TestClientFactory.build()
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .connectionSetupTimeoutMillis(100)
                .maxWaitForConnection(150)
                .create();

        final Client.ClusteredClient client = cluster.connect();

        for (int i = 0; i < 3; i++) {
            try {
                client.submitAsync("Thread.sleep(5000);");
            } catch (Exception e) {
                final Throwable root = ExceptionHelper.getRootCause(e);
                assertTrue(root instanceof TimeoutException);
                assertTrue(root.getMessage().contains(Client.TOO_MANY_IN_FLIGHT_REQUESTS));
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
                client.submit("1+1").all().get(3000, TimeUnit.MILLISECONDS);
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
                    final List<Result> results = client.submit("1+1").all().get(3000, TimeUnit.MILLISECONDS);
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

    @Test
    public void shouldFailOnInitiallyDeadHostForSessionClient() throws Exception {
        testShouldFailOnInitiallyDeadHost(false);
    }
}
