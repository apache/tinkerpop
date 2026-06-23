/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import nl.altindag.log.LogCaptor;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.socket.server.SimpleTestServer;
import org.apache.tinkerpop.gremlin.socket.server.SocketServerConstants;
import org.apache.tinkerpop.gremlin.socket.server.TestHttpServerInitializer;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClientBehaviorIntegrateTest {

    private static final int PORT = 45943;

    private static SimpleTestServer server;
    private static Cluster cluster;
    private static Client client;
    private static LogCaptor logCaptor;

    @BeforeClass
    public static void setUp() throws InterruptedException {
        server = new SimpleTestServer(PORT);
        server.start(new TestHttpServerInitializer());

        cluster = buildCluster().create();
        client = cluster.connect();

        logCaptor = LogCaptor.forClass(ConnectionPool.class);
        final Logger poolLogger = (Logger) LoggerFactory.getLogger(ConnectionPool.class);
        poolLogger.setLevel(Level.DEBUG);
    }

    @AfterClass
    public static void tearDown() {
        if (client != null) client.close();
        if (cluster != null) cluster.close();
        if (server != null) server.stop();
        logCaptor.close();
    }

    private static Cluster.Builder buildCluster() {
        return Cluster.build("localhost")
                .validationRequest(SocketServerConstants.GREMLIN_SINGLE_VERTEX)
                .port(PORT)
                .serializer(new GraphBinaryMessageSerializerV4());
    }

    private static void clearLogs() {
        logCaptor.clearLogs();
    }

    @Test
    public void shouldReceiveSingleVertex() throws Exception {
        final List<Result> results = client.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX).all().get();
        assertEquals(1, results.size());
        assertThat(results.get(0).getObject(), instanceOf(Vertex.class));
    }

    @Test
    public void shouldHandleServerClosingConnectionBeforeResponse() throws Exception {
        try {
            client.submit(SocketServerConstants.GREMLIN_CLOSE_CONNECTION).all().get();
            fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            // ExecutionException -> IllegalStateException or IOException
            final Throwable cause = e.getCause();
            assertTrue("Expected IllegalStateException or IOException but got " + cause.getClass().getName(),
                    cause instanceof IllegalStateException || cause instanceof IOException);
        }

        // driver should recover
        final List<Result> results = client.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX).all().get();
        assertEquals(1, results.size());
        assertThat(results.get(0).getObject(), instanceOf(Vertex.class));
    }

    @Test
    public void shouldHandleSuccessiveConnectionClosures() throws Exception {
        try {
            client.submit(SocketServerConstants.GREMLIN_CLOSE_CONNECTION).all().get();
            fail("Expected ExecutionException");
        } catch (ExecutionException ignored) {
        }

        try {
            client.submit(SocketServerConstants.GREMLIN_CLOSE_CONNECTION).all().get();
            fail("Expected ExecutionException");
        } catch (ExecutionException ignored) {
        }

        // driver should recover after multiple failures
        final List<Result> results = client.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX).all().get();
        assertEquals(1, results.size());
        assertThat(results.get(0).getObject(), instanceOf(Vertex.class));
    }

    @Test
    public void shouldHandleAsyncRequestsDuringConnectionClose() throws Exception {
        clearLogs();

        final CompletableFuture<List<Result>> f1 = client.submit(SocketServerConstants.GREMLIN_CLOSE_CONNECTION).all();
        final CompletableFuture<List<Result>> f2 = client.submit(SocketServerConstants.GREMLIN_CLOSE_CONNECTION).all();

        try {
            f1.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException ignored) {
        }

        try {
            f2.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException ignored) {
        }

        // wait for pool stabilization
        Thread.sleep(2000);

        // verify the pool considered/created a replacement connection
        assertTrue("Expected pool to replace or consider new connection",
                logCaptor.getLogs().stream().anyMatch(msg ->
                        msg.contains("Replace ") || msg.contains("Considering new connection")));

        // verify recovery
        final List<Result> results = client.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX).all().get();
        assertEquals(1, results.size());
        assertThat(results.get(0).getObject(), instanceOf(Vertex.class));
    }

    @Test
    public void shouldHandleServerClosingConnectionAfterResponse() throws Exception {
        clearLogs();

        final List<Result> results = client.submit(SocketServerConstants.GREMLIN_VERTEX_THEN_CLOSE).all().get();
        assertEquals(1, results.size());
        assertThat(results.get(0).getObject(), instanceOf(Vertex.class));

        // wait for the delayed close to happen
        Thread.sleep(3000);

        // connection pool should create a new connection
        final List<Result> results2 = client.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX).all().get();
        assertEquals(1, results2.size());
        assertThat(results2.get(0).getObject(), instanceOf(Vertex.class));

        // verify the pool dealt with the dead connection
        assertTrue("Expected pool to replace or destroy the dead connection",
                logCaptor.getLogs().stream().anyMatch(msg -> msg.contains("Replace ") || msg.contains("Destroyed ")));
    }

    @Test
    public void shouldHandleServerErrorAfterDelay() throws Exception {
        try {
            client.submit(SocketServerConstants.GREMLIN_FAIL_AFTER_DELAY).all().get();
            fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(ResponseException.class));
            final ResponseException re = (ResponseException) e.getCause();
            assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, re.getResponseStatusCode());
            assertTrue(re.getMessage().contains("Server error"));
        }

        // A 500 is a valid HTTP response, connection should still be usable
        final List<Result> results = client.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX).all().get();
        assertEquals(1, results.size());
    }

    @Test
    public void shouldHandlePartialContentClose() throws Exception {
        try {
            client.submit(SocketServerConstants.GREMLIN_PARTIAL_CONTENT_CLOSE).all().get();
            fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(IllegalStateException.class));
            assertTrue(e.getCause().getMessage().contains("no longer active"));
        }

        // driver should recover with a new connection
        final List<Result> results = client.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX).all().get();
        assertEquals(1, results.size());
    }

    @Test
    public void shouldHandleMalformedResponse() throws Exception {
        try {
            client.submit(SocketServerConstants.GREMLIN_MALFORMED_RESPONSE).all().get();
            fail("Expected ExecutionException");
        } catch (ExecutionException ignored) {
        }

        // connection should still be usable or pool creates new one
        final List<Result> results = client.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX).all().get();
        assertEquals(1, results.size());
        assertThat(results.get(0).getObject(), instanceOf(Vertex.class));
    }

    @Test
    public void shouldHandleEmptyResponseBody() throws Exception {
        try {
            client.submit(SocketServerConstants.GREMLIN_EMPTY_BODY).all().get(5, TimeUnit.SECONDS);
            fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            // Empty body causes the stream reader to hit EOF immediately
            assertThat(e.getCause(), instanceOf(RuntimeException.class));
            assertThat(e.getCause().getCause(), instanceOf(java.io.EOFException.class));
            assertTrue(e.getCause().getMessage().contains("EOFException"));
        } catch (TimeoutException e) {
            fail("Driver hung on empty response body instead of throwing an error");
        }

        // driver should recover
        final List<Result> results = client.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX).all().get();
        assertEquals(1, results.size());
    }

    @Test
    public void shouldHandleSlowResponse() throws Exception {
        final List<Result> results = client.submit(SocketServerConstants.GREMLIN_SLOW_RESPONSE)
                .all().get(10, TimeUnit.SECONDS);
        assertEquals(3, results.size());
    }

    @Test
    public void shouldTimeoutWhenServerNeverResponds() throws Exception {
        final Cluster timeoutCluster = buildCluster()
                .idleTimeoutMillis(2000)
                .create();
        final Client timeoutClient = timeoutCluster.connect();
        try {
            try {
                timeoutClient.submit(SocketServerConstants.GREMLIN_NO_RESPONSE).all().get(5, TimeUnit.SECONDS);
                fail("Expected ExecutionException");
            } catch (ExecutionException e) {
                assertThat(e.getCause(), instanceOf(RuntimeException.class));
                assertTrue(e.getCause().getMessage().contains("Idle timeout occurred before response could be received"));
            }

            // same client should recover - pool replaces the dead connection
            final List<Result> results = timeoutClient.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX).all().get();
            assertEquals(1, results.size());
        } finally {
            timeoutClient.close();
            timeoutCluster.close();
        }
    }

    @Test
    public void shouldRecoverFromTemporaryServerDowntime() throws Exception {
        // Use a dedicated cluster with short reconnect interval for this test
        final Cluster recoveryCluster = buildCluster().reconnectInterval(500).create();
        final Client recoveryClient = recoveryCluster.connect();
        try {
            // First verify the client works
            final List<Result> before = recoveryClient.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX)
                    .all().get(5, TimeUnit.SECONDS);
            assertEquals(1, before.size());

            // Stop the server — existing connections become dead
            server.stopSync();
            try {
                // Attempt request with the SAME client — expect failure
                try {
                    recoveryClient.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX).all().get(5, TimeUnit.SECONDS);
                    fail("Should have thrown exception while server is down");
                } catch (RuntimeException e) {
                    assertThat(e.getCause(), instanceOf(TimeoutException.class));
                    assertTrue(e.getCause().getMessage().contains("Connection refused"));
                }
            } finally {
                // Restart the server
                server.start(new TestHttpServerInitializer());
            }

            // Wait for the driver's reconnection logic to detect the server is back
            Thread.sleep(3000);

            // Verify the SAME client self-heals and can submit successfully again
            final List<Result> after = recoveryClient.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX)
                    .all().get(30, TimeUnit.SECONDS);
            assertEquals(1, after.size());
        } finally {
            recoveryClient.close();
            recoveryCluster.close();
        }
    }
    @Test
    public void shouldHandleConcurrentMixedRequests() throws Exception {
        final int count = 5;
        final ExecutorService executor = Executors.newFixedThreadPool(count * 2);

        // Use a fresh cluster with enough pool size
        final Cluster concurrentCluster = buildCluster().maxConnections(count * 2).create();
        final Client concurrentClient = concurrentCluster.connect();

        try {
            // Submit good and bad requests concurrently, tracking them separately
            final List<Future<List<Result>>> goodFutures = new ArrayList<>();
            final List<Future<List<Result>>> badFutures = new ArrayList<>();

            for (int i = 0; i < count; i++) {
                goodFutures.add(executor.submit(() ->
                        concurrentClient.submit(SocketServerConstants.GREMLIN_SINGLE_VERTEX).all().get(10, TimeUnit.SECONDS)));
                badFutures.add(executor.submit(() ->
                        concurrentClient.submit(SocketServerConstants.GREMLIN_CLOSE_CONNECTION).all().get(10, TimeUnit.SECONDS)));
            }

            // All good requests should succeed
            for (final Future<List<Result>> f : goodFutures) {
                final List<Result> results = f.get(15, TimeUnit.SECONDS);
                assertEquals(1, results.size());
                assertThat(results.get(0).getObject(), instanceOf(Vertex.class));
            }

            // All bad requests should fail
            for (final Future<List<Result>> f : badFutures) {
                try {
                    f.get(15, TimeUnit.SECONDS);
                    fail("Expected exception for close-connection request");
                } catch (ExecutionException expected) {
                    // Expected — server closed the connection
                }
            }
        } finally {
            concurrentClient.close();
            concurrentCluster.close();
            executor.shutdown();
        }
    }
}
