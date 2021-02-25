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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Level;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketClientHandler;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.driver.ser.JsonBuilderGryoSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.handler.OpExecutorHandler;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.util.Log4jRecordingAppender;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import groovy.json.JsonBuilder;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.hamcrest.core.IsInstanceOf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Color;
import java.io.File;
import java.time.Instant;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.mockito.Mockito.verify;

/**
 * Integration tests for gremlin-driver configurations and settings.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinDriverIntegrateTest extends AbstractGremlinServerIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(GremlinDriverIntegrateTest.class);

    private Log4jRecordingAppender recordingAppender = null;
    private Level previousLogLevel;

    @Before
    public void setupForEachTest() {
        recordingAppender = new Log4jRecordingAppender();
        final org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();

        if (name.getMethodName().equals("shouldKeepAliveForWebSockets") ||
                name.getMethodName().equals("shouldKeepAliveForWebSocketsWithNoInFlightRequests")) {
            final org.apache.log4j.Logger webSocketClientHandlerLogger = org.apache.log4j.Logger.getLogger(WebSocketClientHandler.class);
            previousLogLevel = webSocketClientHandlerLogger.getLevel();
            webSocketClientHandlerLogger.setLevel(Level.DEBUG);
        } else if (name.getMethodName().equals("shouldEventuallySucceedAfterMuchFailure")) {
            final org.apache.log4j.Logger opExecutorHandlerLogger = org.apache.log4j.Logger.getLogger(OpExecutorHandler.class);
            previousLogLevel = opExecutorHandlerLogger.getLevel();
            opExecutorHandlerLogger.setLevel(Level.ERROR);
        }

        rootLogger.addAppender(recordingAppender);
    }

    @After
    public void teardownForEachTest() {
        final org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();

        if (name.getMethodName().equals("shouldKeepAliveForWebSockets") ||
                name.getMethodName().equals("shouldKeepAliveForWebSocketsWithNoInFlightRequests")) {
            final org.apache.log4j.Logger webSocketClientHandlerLogger = org.apache.log4j.Logger.getLogger(WebSocketClientHandler.class);
            webSocketClientHandlerLogger.setLevel(previousLogLevel);
        } else if (name.getMethodName().equals("shouldEventuallySucceedAfterMuchFailure")) {
            final org.apache.log4j.Logger opExecutorHandlerLogger = org.apache.log4j.Logger.getLogger(OpExecutorHandler.class);
            opExecutorHandlerLogger.setLevel(previousLogLevel);
        }

        rootLogger.removeAppender(recordingAppender);
    }

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final String nameOfTest = name.getMethodName();

        switch (nameOfTest) {
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
            case "shouldManageTransactionsInSession":
                deleteDirectory(new File("/tmp/neo4j"));
                settings.graphs.put("graph", "conf/neo4j-empty.properties");
                break;
            case "shouldRequireAliasedGraphVariablesInStrictTransactionMode":
                settings.strictTransactionManagement = true;
                break;
            case "shouldAliasGraphVariablesInStrictTransactionMode":
                settings.strictTransactionManagement = true;
                deleteDirectory(new File("/tmp/neo4j"));
                settings.graphs.put("graph", "conf/neo4j-empty.properties");
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
    public void shouldReportErrorWhenRequestCantBeSerialized() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V3D0).create();
        try {
            final Client client = cluster.connect().alias("g");

            try {
                final Map<String, Object> params = new HashMap<>();
                params.put("r", Color.RED);
                client.submit("r", params).all().get();
                fail("Should have thrown exception over bad serialization");
            } catch (Exception ex) {
                final Throwable inner = ExceptionUtils.getRootCause(ex);
                assertThat(inner, instanceOf(ResponseException.class));
                assertEquals(ResponseStatusCode.REQUEST_ERROR_SERIALIZATION, ((ResponseException) inner).getResponseStatusCode());
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
    public void shouldProcessTraversalInterruption() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("g.inject(1).sideEffect{Thread.sleep(5000)}").all().get();
            fail("Should have timed out");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(ResponseStatusCode.SERVER_ERROR_TIMEOUT, re.getResponseStatusCode());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldProcessEvalInterruption() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("Thread.sleep(5000);'done'").all().get();
            fail("Should have timed out");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(ResponseStatusCode.SERVER_ERROR_TIMEOUT, re.getResponseStatusCode());
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
            final long messages = recordingAppender.getMessages().stream().filter(m -> m.contains("Sending ping frame to the server")).count();
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
            final long messages = recordingAppender.getMessages().stream().filter(m -> m.contains("Sending ping frame to the server")).count();
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
                final Throwable root = ExceptionUtils.getRootCause(ex);
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
                    final Throwable root = ExceptionUtils.getRootCause(ex);
                    assertThat(root, instanceOf(ResponseException.class));
                }
            });

            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());

        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEventuallySucceedOnSameServerWithDefault() throws Exception {
        stopServer();

        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            try {
                client.submit("1+1").all().join().get(0).getInt();
                fail("Should not have gone through because the server is not running");
            } catch (Exception i) {
                final Throwable root = ExceptionUtils.getRootCause(i);
                assertThat(root, instanceOf(NoHostAvailableException.class));
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
                    logger.warn("Attempt {} failed on shouldEventuallySucceedOnSameServerWithDefault", ix);
                }
            }

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
            final Throwable root = ExceptionUtils.getRootCause(i);
            assertThat(root, instanceOf(NoHostAvailableException.class));
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
                final Throwable inner = ExceptionUtils.getRootCause(ex);
                assertThat(inner, instanceOf(ResponseException.class));
                assertEquals(ResponseStatusCode.SERVER_ERROR_SERIALIZATION, ((ResponseException) inner).getResponseStatusCode());
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
                fail("Should have thrown exception over bad serialization");
            } catch (Exception ex) {
                final Throwable inner = ExceptionUtils.getRootCause(ex);
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

    /**
     * This test validates that the session requests are processed in-order on the server. The order of results
     * returned to the client might be different though since each result is handled by a different executor thread.
     */
    @Test
    public void shouldProcessSessionRequestsInOrder() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        try {
            final Client client = cluster.connect(name.getMethodName());

            final ResultSet first = client.submit("Thread.sleep(5000);g.V().fold().coalesce(unfold(), __.addV('person'))");
            final ResultSet second = client.submit("g.V().count()");

            final CompletableFuture<List<Result>> futureFirst = first.all();
            final CompletableFuture<List<Result>> futureSecond = second.all();

            final CountDownLatch latch = new CountDownLatch(2);
            final List<Object> results = new ArrayList<>();
            final ExecutorService executor = Executors.newSingleThreadExecutor();

            futureFirst.thenAcceptAsync(r -> {
                results.add(r.get(0).getVertex().label());
                latch.countDown();
            }, executor);

            futureSecond.thenAcceptAsync(r -> {
                results.add(r.get(0).getLong());
                latch.countDown();
            }, executor);

            // wait for both results
            latch.await(30000, TimeUnit.MILLISECONDS);

            assertThat("Should contain 2 results", results.size() == 2);
            assertThat("The numeric result should be 1", results.contains(1L));
            assertThat("The string result contain label person", results.contains("person"));
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
    public void shouldStream() throws Exception {
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
                final Throwable inner = ExceptionUtils.getRootCause(ex);
                assertTrue(inner instanceof ResponseException);
                assertEquals(ResponseStatusCode.SERVER_ERROR_SERIALIZATION, ((ResponseException) inner).getResponseStatusCode());
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
    public void shouldSerializeToStringWhenRequestedGryoV1() throws Exception {
        final Map<String, Object> m = new HashMap<>();
        m.put("serializeResultToString", true);
        final GryoMessageSerializerV1d0 serializer = new GryoMessageSerializerV1d0();
        serializer.configure(m, null);

        final Cluster cluster = TestClientFactory.build().serializer(serializer).create();
        final Client client = cluster.connect();

        try {
            final ResultSet resultSet = client.submit("TinkerFactory.createClassic()");
            final List<Result> results = resultSet.all().join();
            assertEquals(1, results.size());
            assertEquals("tinkergraph[vertices:6 edges:6]", results.get(0).getString());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldSerializeToStringWhenRequestedGryoV3() throws Exception {
        final Map<String, Object> m = new HashMap<>();
        m.put("serializeResultToString", true);
        final GryoMessageSerializerV3d0 serializer = new GryoMessageSerializerV3d0();
        serializer.configure(m, null);

        final Cluster cluster = TestClientFactory.build().serializer(serializer).create();
        final Client client = cluster.connect();

        try {
            final ResultSet resultSet = client.submit("TinkerFactory.createClassic()");
            final List<Result> results = resultSet.all().join();
            assertEquals(1, results.size());
            assertEquals("tinkergraph[vertices:6 edges:6]", results.get(0).getString());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldDeserializeWithCustomClassesV1() throws Exception {
        final Map<String, Object> m = new HashMap<>();
        m.put("custom", Collections.singletonList(String.format("%s;%s", JsonBuilder.class.getCanonicalName(), JsonBuilderGryoSerializer.class.getCanonicalName())));
        final GryoMessageSerializerV1d0 serializer = new GryoMessageSerializerV1d0();
        serializer.configure(m, null);

        final Cluster cluster = TestClientFactory.build().serializer(serializer).create();
        final Client client = cluster.connect();

        try {
            final List<Result> json = client.submit("b = new groovy.json.JsonBuilder();b.people{person {fname 'stephen'\nlname 'mallette'}};b").all().join();
            assertEquals("{\"people\":{\"person\":{\"fname\":\"stephen\",\"lname\":\"mallette\"}}}", json.get(0).getString());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldDeserializeWithCustomClassesV3() throws Exception {
        final Map<String, Object> m = new HashMap<>();
        m.put("custom", Collections.singletonList(String.format("%s;%s", JsonBuilder.class.getCanonicalName(), JsonBuilderGryoSerializer.class.getCanonicalName())));
        final GryoMessageSerializerV3d0 serializer = new GryoMessageSerializerV3d0();
        serializer.configure(m, null);

        final Cluster cluster = TestClientFactory.build().serializer(serializer).create();
        final Client client = cluster.connect();

        try {
            final List<Result> json = client.submit("b = new groovy.json.JsonBuilder();b.people{person {fname 'stephen'\nlname 'mallette'}};b").all().join();
            assertEquals("{\"people\":{\"person\":{\"fname\":\"stephen\",\"lname\":\"mallette\"}}}", json.get(0).getString());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldWorkWithGraphSONV1Serialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V1D0).create();
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
            assertEquals(0l, nameProperties.get("id"));
            assertEquals("marko", nameProperties.get("value"));

            final List<Object> ages = (List<Object>) properties.get("age");
            assertEquals(1, ages.size());

            final Map<String, Object> ageProperties = (Map<String, Object>) ages.get(0);
            assertEquals(2, ageProperties.size());
            assertEquals(1l, ageProperties.get("id"));
            assertEquals(29, ageProperties.get("value"));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldWorkWithGraphSONV2Serialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V2D0).create();
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

    @Test
    public void shouldWorkWithGraphSONExtendedV2Serialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V2D0).create();
        final Client client = cluster.connect();

        try {
            final List<Result> r = client.submit("java.time.Instant.EPOCH").all().join();
            assertEquals(1, r.size());

            final Instant then = r.get(0).get(Instant.class);
            assertEquals(Instant.EPOCH, then);
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldWorkWithGraphSONV3Serialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V3D0).create();
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

    @Test
    public void shouldWorkWithGraphSONExtendedV3Serialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V3D0).create();
        final Client client = cluster.connect();

        try {
            final List<Result> r = client.submit("java.time.Instant.EPOCH").all().join();
            assertEquals(1, r.size());

            final Instant then = r.get(0).get(Instant.class);
            assertEquals(Instant.EPOCH, then);
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldWorkWithGraphBinaryV1Serialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHBINARY_V1D0).create();
        final Client client = cluster.connect();

        try {
            final List<Result> r = client.submit("TinkerFactory.createModern().traversal().V(1)").all().join();
            assertEquals(1, r.size());

            final Vertex v = r.get(0).get(ReferenceVertex.class);
            assertEquals(1, v.id());
            assertEquals("person", v.label());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailClientSideWithTooLargeAResponse() {
        final Cluster cluster = TestClientFactory.build().maxContentLength(1).create();
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
    public void shouldReturnNiceMessageFromOpSelector() {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            final Map m = new HashMap<>();
            m.put(null, "a null key will force a throw of OpProcessorException in message validation");
            client.submit("1+1", m).all().get();
            fail("Should throw an exception.");
        } catch (Exception re) {
            final Throwable root = ExceptionUtils.getRootCause(re);
            assertEquals("The [eval] message is using one or more invalid binding keys - they must be of type String and cannot be null", root.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldExecuteScriptInSession() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
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
    public void shouldCloseSession() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect(name.getMethodName());

        final ResultSet results1 = client.submit("x = [1,2,3,4,5,6,7,8,9]");
        assertEquals(9, results1.all().get().size());
        final ResultSet results2 = client.submit("x[0]+1");
        assertEquals(2, results2.all().get().get(0).getInt());

        client.close();

        try {
            client.submit("x[0]+1").all().get();
            fail("Should have thrown an exception because the connection is closed");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldExecuteScriptInSessionAssumingDefaultedImports() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect(name.getMethodName());

        final ResultSet results1 = client.submit("TinkerFactory.class.name");
        assertEquals(TinkerFactory.class.getName(), results1.all().get().get(0).getString());

        cluster.close();
    }

    @Test
    public void shouldExecuteScriptInSessionOnTransactionalGraph() throws Exception {
        assumeNeo4jIsPresent();

        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect(name.getMethodName());

        final Vertex vertexBeforeTx = client.submit("v=g.addV(\"person\").property(\"name\",\"stephen\").next()").all().get().get(0).getVertex();
        assertEquals("person", vertexBeforeTx.label());

        final String nameValueFromV = client.submit("g.V().values('name').next()").all().get().get(0).getString();
        assertEquals("stephen", nameValueFromV);

        final Vertex vertexFromBinding = client.submit("v").all().get().get(0).getVertex();
        assertEquals("person", vertexFromBinding.label());

        final Map<String,Object> vertexAfterTx = client.submit("g.V(v).property(\"color\",\"blue\").iterate(); g.tx().commit(); g.V(v).valueMap().by(unfold())").all().get().get(0).get(Map.class);
        assertEquals("stephen", vertexAfterTx.get("name"));
        assertEquals("blue", vertexAfterTx.get("color"));

        cluster.close();
    }

    @Test
    public void shouldExecuteScriptInSessionOnTransactionalWithManualTransactionsGraph() throws Exception {
        assumeNeo4jIsPresent();

        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect(name.getMethodName());
        final Client sessionlessClient = cluster.connect();
        client.submit("graph.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);null").all().get();
        client.submit("graph.tx().open()").all().get();

        final Vertex vertexBeforeTx = client.submit("v=g.addV(\"person\").property(\"name\", \"stephen\").next()").all().get().get(0).getVertex();
        assertEquals("person", vertexBeforeTx.label());

        final String nameValueFromV = client.submit("g.V().values(\"name\").next()").all().get().get(0).getString();
        assertEquals("stephen", nameValueFromV);

        final Vertex vertexFromBinding = client.submit("v").all().get().get(0).getVertex();
        assertEquals("person", vertexFromBinding.label());

        client.submit("g.V(v).property(\"color\",\"blue\")").all().get();
        client.submit("g.tx().commit()").all().get();

        // Run a sessionless request to change transaction.readWriteConsumer back to AUTO
        // The will make the next in session request fail if consumers aren't ThreadLocal
        sessionlessClient.submit("g.V().next()").all().get();

        client.submit("g.tx().open()").all().get();

        final Map<String,Object> vertexAfterTx = client.submit("g.V().valueMap().by(unfold())").all().get().get(0).get(Map.class);
        assertEquals("stephen", vertexAfterTx.get("name"));
        assertEquals("blue", vertexAfterTx.get("color"));

        client.submit("g.tx().rollback()").all().get();

        cluster.close();
    }

    @Test
    public void shouldExecuteInSessionAndSessionlessWithoutOpeningTransaction() throws Exception {
        assumeNeo4jIsPresent();

        final Cluster cluster = TestClientFactory.open();
        try {
            final Client sessionClient = cluster.connect(name.getMethodName());
            final Client sessionlessClient = cluster.connect();

            //open transaction in session, then add vertex and commit
            sessionClient.submit("g.tx().open()").all().get();
            final Vertex vertexBeforeTx = sessionClient.submit("v=g.addV(\"person\").property(\"name\",\"stephen\").next()").all().get().get(0).getVertex();
            assertEquals("person", vertexBeforeTx.label());
            sessionClient.submit("g.tx().commit()").all().get();

            // check that session transaction is closed
            final boolean isOpen = sessionClient.submit("g.tx().isOpen()").all().get().get(0).getBoolean();
            assertTrue("Transaction should be closed", !isOpen);

            //run a sessionless read
            sessionlessClient.submit("g.V()").all().get();

            // check that session transaction is still closed
            final boolean isOpenAfterSessionless = sessionClient.submit("g.tx().isOpen()").all().get().get(0).getBoolean();
            assertTrue("Transaction should stil be closed", !isOpenAfterSessionless);
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldExecuteSessionlessScriptOnTransactionalGraph() throws Exception {
        assumeNeo4jIsPresent();

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
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, IsInstanceOf.instanceOf(ResponseException.class));
            assertThat(root.getMessage(), containsString("No such property: x for class"));
        }

        try {
            client2.submit("x").all().get();
            fail("The variable 'x' should not be present on the new request.");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, IsInstanceOf.instanceOf(ResponseException.class));
            assertThat(root.getMessage(), containsString("No such property: x for class"));
        }

        try {
            client3.submit("x").all().get();
            fail("The variable 'x' should not be present on the new request.");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, IsInstanceOf.instanceOf(ResponseException.class));
            assertThat(root.getMessage(), containsString("No such property: x for class"));
        }

        cluster.close();
    }

    @Test
    public void shouldBeThreadSafeToUseOneClient() throws Exception {
        final Cluster cluster = TestClientFactory.build().workerPoolSize(2)
                .maxInProcessPerConnection(64)
                .minInProcessPerConnection(32)
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
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            final ResponseException re = (ResponseException) root;
            assertEquals(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS, re.getResponseStatusCode());
        }

        cluster.close();
    }

    @Test
    public void shouldAliasGraphVariablesInStrictTransactionMode() throws Exception {
        assumeNeo4jIsPresent();

        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("g.addVertex('name','stephen');").all().get().get(0).getVertex();
            fail("Should have tossed an exception because \"g\" does not have the addVertex method under default config");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            final ResponseException re = (ResponseException) root;
            assertEquals(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS, re.getResponseStatusCode());
        }

        final Client rebound = cluster.connect().alias("graph");
        final Vertex v = rebound.submit("g.addVertex(T.label,'person')").all().get().get(0).getVertex();
        assertEquals("person", v.label());

        cluster.close();
    }

    @Test
    public void shouldAliasGraphVariables() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("g.addVertex(label,'person','name','stephen');").all().get().get(0).getVertex();
            fail("Should have tossed an exception because \"g\" does not have the addVertex method under default config");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            final ResponseException re = (ResponseException) root;
            assertEquals(ResponseStatusCode.SERVER_ERROR_EVALUATION, re.getResponseStatusCode());
        }

        final Client rebound = cluster.connect().alias("graph");
        final Vertex v = rebound.submit("g.addVertex(label,'person','name','jason')").all().get().get(0).getVertex();
        assertEquals("person", v.label());

        cluster.close();
    }

    @Test
    public void shouldAliasTraversalSourceVariables() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRYO_V3D0).create();
        final Client client = cluster.connect();
        try {
            try {
                client.submit("g.addV().property('name','stephen')").all().get().get(0).getVertex();
                fail("Should have tossed an exception because \"g\" is readonly in this context");
            } catch (Exception ex) {
                final Throwable root = ExceptionUtils.getRootCause(ex);
                assertThat(root, instanceOf(ResponseException.class));
                final ResponseException re = (ResponseException) root;
                assertEquals(ResponseStatusCode.SERVER_ERROR_EVALUATION, re.getResponseStatusCode());
            }

            final Client clientAliased = client.alias("g1");
            final Vertex v = clientAliased.submit("g.addV().property('name','jason')").all().get().get(0).getVertex();
            assertEquals("jason", v.value("name"));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAliasGraphVariablesInSession() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRYO_V3D0).create();
        final Client client = cluster.connect(name.getMethodName());

        try {
            client.submit("g.addVertex('name','stephen');").all().get().get(0).getVertex();
            fail("Should have tossed an exception because \"g\" does not have the addVertex method under default config");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            final ResponseException re = (ResponseException) root;
            assertEquals(ResponseStatusCode.SERVER_ERROR_EVALUATION, re.getResponseStatusCode());
        }

        final Client aliased = client.alias("graph");
        assertEquals("jason", aliased.submit("n='jason'").all().get().get(0).getString());
        final Vertex v = aliased.submit("g.addVertex('name',n)").all().get().get(0).getVertex();
        assertEquals("jason", v.value("name"));

        cluster.close();
    }

    @Test
    public void shouldAliasTraversalSourceVariablesInSession() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRYO_V3D0).create();
        final Client client = cluster.connect(name.getMethodName());

        try {
            client.submit("g.addV().property('name','stephen')").all().get().get(0).getVertex();
            fail("Should have tossed an exception because \"g\" is readonly in this context");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            final ResponseException re = (ResponseException) root;
            assertEquals(ResponseStatusCode.SERVER_ERROR_EVALUATION, re.getResponseStatusCode());
        }

        final Client clientAliased = client.alias("g1");
        assertEquals("jason", clientAliased.submit("n='jason'").all().get().get(0).getString());
        final Vertex v = clientAliased.submit("g.addV().property('name',n)").all().get().get(0).getVertex();
        assertEquals("jason", v.value("name"));

        cluster.close();
    }

    @Test
    public void shouldManageTransactionsInSession() throws Exception {
        assumeNeo4jIsPresent();

        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();
        final Client sessionWithManagedTx = cluster.connect(name.getMethodName() + "-managed", true);
        final Client sessionWithoutManagedTx = cluster.connect(name.getMethodName() + "-not-managed");

        // this should auto-commit
        sessionWithManagedTx.submit("v = g.addV().property('name','stephen').next()").all().get().get(0).getVertex();

        // the other clients should see that change because of auto-commit
        assertThat(client.submit("g.V().has('name','stephen').hasNext()").all().get().get(0).getBoolean(), is(true));
        assertThat(sessionWithoutManagedTx.submit("g.V().has('name','stephen').hasNext()").all().get().get(0).getBoolean(), is(true));

        // this should NOT auto-commit
        final Vertex vDaniel = sessionWithoutManagedTx.submit("v = g.addV().property('name','daniel').next()").all().get().get(0).getVertex();

        // the other clients should NOT see that change because of auto-commit
        assertThat(client.submit("g.V().has('name','daniel').hasNext()").all().get().get(0).getBoolean(), is(false));
        assertThat(sessionWithManagedTx.submit("g.V().has('name','daniel').hasNext()").all().get().get(0).getBoolean(), is(false));

        // but "v" should still be there
        final Vertex vDanielAgain = sessionWithoutManagedTx.submit("v").all().get().get(0).getVertex();
        assertEquals(vDaniel.id(), vDanielAgain.id());

        // now commit manually
        sessionWithoutManagedTx.submit("g.tx().commit()").all().get();

        // should be there for all now
        assertThat(client.submit("g.V().has('name','daniel').hasNext()").all().get().get(0).getBoolean(), is(true));
        assertThat(sessionWithManagedTx.submit("g.V().has('name','daniel').hasNext()").all().get().get(0).getBoolean(), is(true));
        assertThat(sessionWithoutManagedTx.submit("g.V().has('name','daniel').hasNext()").all().get().get(0).getBoolean(), is(true));

        cluster.close();
    }

    @Test
    public void shouldProcessSessionRequestsInOrderAfterTimeout() throws Exception {
        final Cluster cluster = TestClientFactory.open();

        try {
            final Client client = cluster.connect(name.getMethodName());

            for (int index = 0; index < 50; index++) {
                final CompletableFuture<ResultSet> first = client.submitAsync(
                        "Object mon1 = 'mon1';\n" +
                                "synchronized (mon1) {\n" +
                                "    mon1.wait();\n" +
                                "} ");

                final CompletableFuture<ResultSet> second = client.submitAsync(
                        "Object mon2 = 'mon2';\n" +
                                "synchronized (mon2) {\n" +
                                "    mon2.wait();\n" +
                                "}");

                final CompletableFuture<ResultSet> third = client.submitAsync(
                        "Object mon3 = 'mon3';\n" +
                                "synchronized (mon3) {\n" +
                                "    mon3.wait();\n" +
                                "}");

                final CompletableFuture<ResultSet> fourth = client.submitAsync(
                        "Object mon4 = 'mon4';\n" +
                                "synchronized (mon4) {\n" +
                                "    mon4.wait();\n" +
                                "}");

                final CompletableFuture<List<Result>> futureFirst = first.get().all();
                final CompletableFuture<List<Result>> futureSecond = second.get().all();
                final CompletableFuture<List<Result>> futureThird = third.get().all();
                final CompletableFuture<List<Result>> futureFourth = fourth.get().all();

                assertFutureTimeout(futureFirst);
                assertFutureTimeout(futureSecond);
                assertFutureTimeout(futureThird);
                assertFutureTimeout(futureFourth);
            }
        } finally {
            cluster.close();
        }
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
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
            assertEquals("Client has been closed", root.getMessage());
        }

        try {
            session.submit("1+1").all().get();
            fail("Should have tossed an exception because cluster was closed");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
            assertEquals("Client has been closed", root.getMessage());
        }

        try {
            sessionlessTwo.submit("1+1").all().get();
            fail("Should have tossed an exception because cluster was closed");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
            assertEquals("Client has been closed", root.getMessage());
        }

        try {
            sessionlessThree.submit("1+1").all().get();
            fail("Should have tossed an exception because cluster was closed");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
            assertEquals("Client has been closed", root.getMessage());
        }

        try {
            sessionlessFour.submit("1+1").all().get();
            fail("Should have tossed an exception because cluster was closed");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
            assertEquals("Client has been closed", root.getMessage());
        }

        // allow call to close() even though closed through cluster
        sessionlessOne.close();
        session.close();
        sessionlessTwo.close();

        cluster.close();
    }

    @Test
    public void shouldSendUserAgent() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V3D0).create();
        final Client client = Mockito.spy(cluster.connect().alias("g"));
        client.submit("", RequestOptions.build().userAgent("test").create()).all().get();
        cluster.close();
        ArgumentCaptor<RequestMessage> requestMessageCaptor = ArgumentCaptor.forClass(RequestMessage.class);
        verify(client).submitAsync(requestMessageCaptor.capture());
        RequestMessage requestMessage = requestMessageCaptor.getValue();
        assertEquals("test", requestMessage.getArgs().get(Tokens.ARGS_USER_AGENT));
    }

    @Test
    public void shouldSendUserAgentBytecode() {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V3D0).create();
        final Client client = Mockito.spy(cluster.connect().alias("g"));
        Mockito.when(client.alias("g")).thenReturn(client);
        GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(client));
        g.with(Tokens.ARGS_USER_AGENT, "test").V().iterate();
        cluster.close();
        ArgumentCaptor<RequestOptions> requestOptionsCaptor = ArgumentCaptor.forClass(RequestOptions.class);
        verify(client).submitAsync(Mockito.any(Bytecode.class), requestOptionsCaptor.capture());
        RequestOptions requestOptions = requestOptionsCaptor.getValue();
        assertEquals("test", requestOptions.getUserAgent().get());

        ArgumentCaptor<RequestMessage> requestMessageCaptor = ArgumentCaptor.forClass(RequestMessage.class);
        verify(client).submitAsync(requestMessageCaptor.capture());
        RequestMessage requestMessage = requestMessageCaptor.getValue();
        assertEquals("test", requestMessage.getArgs().getOrDefault(Tokens.ARGS_USER_AGENT, null));
    }

    @Test
    public void shouldSendRequestIdBytecode() {
        final UUID overrideRequestId = UUID.randomUUID();
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V3D0).create();
            final Client client = Mockito.spy(cluster.connect().alias("g"));
            Mockito.when(client.alias("g")).thenReturn(client);
            GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(client));
            g.with(Tokens.REQUEST_ID, overrideRequestId).V().iterate();
            cluster.close();
            ArgumentCaptor<RequestOptions> requestOptionsCaptor = ArgumentCaptor.forClass(RequestOptions.class);
            verify(client).submitAsync(Mockito.any(Bytecode.class), requestOptionsCaptor.capture());
            RequestOptions requestOptions = requestOptionsCaptor.getValue();
            assertTrue(requestOptions.getOverrideRequestId().isPresent());
            assertEquals(overrideRequestId, requestOptions.getOverrideRequestId().get());

            ArgumentCaptor<RequestMessage> requestMessageCaptor = ArgumentCaptor.forClass(RequestMessage.class);
            verify(client).submitAsync(requestMessageCaptor.capture());
            RequestMessage requestMessage = requestMessageCaptor.getValue();
            assertEquals(overrideRequestId, requestMessage.getRequestId());

    }

    private void assertFutureTimeout(final CompletableFuture<List<Result>> futureFirst) {
        try
        {
            futureFirst.get();
            fail("Should have timed out");
        }
        catch (Exception ex)
        {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(ResponseException.class));
            assertThat(root.getMessage(), startsWith("Evaluation exceeded the configured 'evaluationTimeout' threshold of 250 ms"));
        }
    }

    @Test
    public void shouldClusterReadFileFromResources() throws Exception {
        final Cluster cluster = Cluster.open(TestClientFactory.RESOURCE_PATH);
        assertTrue(cluster != null);
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
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root.getMessage(), startsWith("There is already a request pending with an id of:"));
        }

        assertEquals(100, result1.get().one().getInt());
    }
}
