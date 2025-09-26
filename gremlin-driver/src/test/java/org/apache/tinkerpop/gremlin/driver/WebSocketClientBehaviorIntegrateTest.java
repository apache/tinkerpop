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
package org.apache.tinkerpop.gremlin.driver;

import nl.altindag.log.LogCaptor;
import org.apache.tinkerpop.gremlin.socket.server.SimpleSocketServer;
import org.apache.tinkerpop.gremlin.socket.server.SocketServerSettings;
import org.apache.tinkerpop.gremlin.socket.server.TestChannelizers;
import org.apache.tinkerpop.gremlin.socket.server.TestWSGremlinInitializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WebSocketClientBehaviorIntegrateTest {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketClientBehaviorIntegrateTest.class);

    @Rule
    public TestName name = new TestName();

    private static LogCaptor logCaptor;

    private final SocketServerSettings settings;

    private SimpleSocketServer server;

    public WebSocketClientBehaviorIntegrateTest() throws IOException {
        settings = SocketServerSettings.read(FileSystems.getDefault().getPath("..","gremlin-tools", "gremlin-socket-server", "conf", "test-ws-gremlin.yaml"));
        settings.SERIALIZER = "GraphSONV2";
    }

    @BeforeClass
    public static void setupLogCaptor() {
        logCaptor = LogCaptor.forRoot();
    }

    @AfterClass
    public static void tearDown() {
        logCaptor.close();
    }

    @Before
    public void setUp() throws InterruptedException {
        logCaptor.clearLogs();

        server = new SimpleSocketServer(settings);
        if (name.getMethodName().equals("shouldAttemptHandshakeForLongerThanDefaultNettySslHandshakeTimeout") ||
                name.getMethodName().equals("shouldPrintCorrectErrorForRegularWebSocketHandshakeTimeout")) {
            server.start(new TestChannelizers.TestWSNoOpInitializer());
        } else if (name.getMethodName().equals("shouldContinueRunningRemainingConnectionsIfServerThrottlesNewConnections") ||
                name.getMethodName().equals("shouldReturnCorrectExceptionIfServerThrottlesNewConnectionsAndMaxWaitExceeded")) {
            server.start(new TestChannelizers.TestConnectionThrottlingInitializer(settings));
        } else {
            server.start(new TestWSGremlinInitializer(settings));
        }
    }

    @After
    public void shutdown() {
        server.stop();
    }

    /**
     * Tests that client is correctly sending user agent during web socket handshake by having the server return
     * the captured user agent.
     */
    @Test
    public void shouldIncludeUserAgentInHandshakeRequest() {
        final Cluster cluster = Cluster.build("localhost").port(settings.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHSON_V2)
                .create();
        final Client.ClusteredClient client = cluster.connect();

        // trigger the testing server to return captured user agent
        String returnedUserAgent = client.submit("1", RequestOptions.build()
                        .overrideRequestId(settings.USER_AGENT_REQUEST_ID).create()).one().getString();
        assertEquals(UserAgent.USER_AGENT, returnedUserAgent);
    }

    /**
     * Tests that no user agent is sent to server when that behaviour is disabled.
     */
    @Test
    public void shouldNotIncludeUserAgentInHandshakeRequestIfDisabled() {
        final Cluster cluster = Cluster.build("localhost").port(settings.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHSON_V2)
                .enableUserAgentOnConnect(false)
                .create();
        final Client.ClusteredClient client = cluster.connect();

        // trigger the testing server to return captured user agent
        String returnedUserAgent = client.submit("1", RequestOptions.build()
                .overrideRequestId(settings.USER_AGENT_REQUEST_ID).create()).one().getString();
        assertEquals("", returnedUserAgent);
    }

    /**
     * Tests that client is correctly requesting permessage deflate compression is used
     */
    @Test
    public void shouldRequestCompressionByDefault() {
        final Cluster cluster = Cluster.build("localhost").port(settings.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHSON_V2)
                .create();
        final Client.ClusteredClient client = cluster.connect();

        // trigger the testing server to return captured sec-websocket-extensions header
        String returnedWsExtensions = client.submit("1", RequestOptions.build()
                .overrideRequestId(settings.SEC_WEBSOCKET_EXTENSIONS).create()).one().getString();
        assertTrue(returnedWsExtensions.contains("permessage-deflate"));
    }

    /**
     * Tests that client is correctly requesting permessage deflate compression is used
     */
    @Test
    public void shouldRequestCompressionWhenEnabled() {
        final Cluster cluster = Cluster.build("localhost").port(settings.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHSON_V2)
                .enableCompression(true)
                .create();
        final Client.ClusteredClient client = cluster.connect();

        // trigger the testing server to return captured sec-websocket-extensions header
        String returnedWsExtensions = client.submit("1", RequestOptions.build()
                .overrideRequestId(settings.SEC_WEBSOCKET_EXTENSIONS).create()).one().getString();
        assertTrue(returnedWsExtensions.contains("permessage-deflate"));
    }

    /**
     * Tests that no user agent is sent to server when that behaviour is disabled.
     */
    @Test
    public void shouldNotRequestCompressionWhenDisabled() {
        final Cluster cluster = Cluster.build("localhost").port(settings.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHSON_V2)
                .enableCompression(false)
                .create();
        final Client.ClusteredClient client = cluster.connect();

        // trigger the testing server to return captured sec-websocket-extensions header
        String returnedWsExtensions = client.submit("1", RequestOptions.build()
                .overrideRequestId(settings.SEC_WEBSOCKET_EXTENSIONS).create()).one().getString();
        assertFalse(returnedWsExtensions.contains("permessage-deflate"));
    }

    /**
     * Constructs a deadlock situation when initializing a {@link Client} object in sessionless form that leads to
     * hanging behavior in low resource environments (TINKERPOP-2504) and for certain configurations of the
     * {@link Cluster} object where there are simply not enough threads to properly allow the {@link Host} and its
     * related {@link ConnectionPool} objects to spin up properly - see TINKERPOP-2550.
     */
    @Test
    public void shouldNotDeadlockOnInitialization() throws Exception {
        // it seems you can add the same host more than once so while kinda weird it is helpful in faithfully
        // recreating the deadlock situation, though it can/will happen with just one host. workerPoolSize at
        // "1" also helps faithfully reproduce the problem though it can happen at larger pool sizes depending
        // on the timing/interleaving of tasks. the larger connection pool sizes may not be required given the
        // other settings at play but again, just trying to make sure the deadlock state is consistently produced
        // and a larger pool size will mean more time to elapse scheduling connection creation tasks which may
        // further improve chances of scheduling conflicts that produce the deadlock.
        //
        // to force this test to a fail state, change ClusteredClient.initializeImplementation() to use the
        // standard Cluster.executor rather than the hostExecutor (which is a single threaded independent thread
        // pool used just for the purpose of initializing the hosts).
        final Cluster cluster = Cluster.build("localhost").
                addContactPoint("localhost").
                addContactPoint("localhost").port(settings.PORT).
                workerPoolSize(1).
                minConnectionPoolSize(32).maxConnectionPoolSize(32).create();

        final AtomicBoolean failed = new AtomicBoolean(false);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                final Client client = cluster.connect();

                // test will hang in init() where the Host and ConnectionPool are started up
                client.init();
            } catch (Exception ex) {
                // should not "fail" - just hang and then timeout during the executor shutdown as there is
                // a deadlock state, but we have this here just in case. a failed assertion of this value
                // below could be interesting
                logger.error("Client initialization failed with exception which was unexpected", ex);
                failed.set(true);
            } finally {
                cluster.close();
            }
        });

        executor.shutdown();

        // 30 seconds should be ample time, even for travis. the deadlock state happens quite immediately in
        // testing and in most situations this test should zip by in subsecond pace
        assertThat(executor.awaitTermination(30, TimeUnit.SECONDS), is(true));
        assertThat(failed.get(), is(false));
    }

    /**
     * Test a scenario when server closes a connection which does not have any active requests. Such connection
     * should be destroyed and replaced by another connection on next request.
     */
    @Test
    public void shouldRemoveConnectionFromPoolWhenServerClose_WithNoPendingRequests() throws InterruptedException {
        final Cluster cluster = Cluster.build("localhost").port(settings.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHSON_V2)
                .create();
        final Client.ClusteredClient client = cluster.connect();

        // Initialize the client preemptively
        client.init();

        // assert number of connections opened
        final ConnectionPool channelPool = client.hostConnectionPools.values().stream().findFirst().get();
        assertEquals(1, channelPool.getConnectionIDs().size());

        final String originalConnectionID = channelPool.getConnectionIDs().iterator().next();
        logger.info("On client init ConnectionIDs: " + channelPool.getConnectionIDs());

        // trigger the testing server to send a WS close frame
        Vertex v = client.submit("1", RequestOptions.build()
                .overrideRequestId(settings.SINGLE_VERTEX_DELAYED_CLOSE_CONNECTION_REQUEST_ID).create())
                .one().getVertex();

        assertNotNull(v);

        // assert connection is not closed yet
        assertEquals(1, channelPool.getConnectionIDs().size());

        // wait for server to send the close WS frame
        Thread.sleep(6000);

        // assert that original connection is not part of the connection pool any more
        assertThat("The original connection should have been closed by the server.",
                channelPool.getConnectionIDs().contains(originalConnectionID), is(false));

        // assert sanity after connection replacement
        v = client.submit("1",
                RequestOptions.build().overrideRequestId(settings.SINGLE_VERTEX_REQUEST_ID).create())
                .one().getVertex();
        assertNotNull(v);
    }

    /**
     * Tests a scenario when the connection a faulty connection replaced by a new connection.
     * Ensures that the creation of a new replacement channel only happens once.
     */
    @Test
    public void shouldRemoveConnectionFromPoolWhenServerClose_WithPendingRequests() throws InterruptedException, ExecutionException {
        final Cluster cluster = Cluster.build("localhost").port(settings.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHSON_V2)
                .create();

        final Client.ClusteredClient client = cluster.connect();

        // Initialize the client preemptively
        client.init();

        // assert number of connections opened
        final ConnectionPool channelPool = client.hostConnectionPools.values().stream().findFirst().get();
        assertEquals(1, channelPool.getConnectionIDs().size());

        // Send two requests in flight. Both should error out.
        final CompletableFuture<ResultSet> req1 = client.submitAsync("1", RequestOptions.build()
                .overrideRequestId(settings.CLOSE_CONNECTION_REQUEST_ID).create());
        final CompletableFuture<ResultSet> req2 = client.submitAsync("1", RequestOptions.build()
                .overrideRequestId(settings.CLOSE_CONNECTION_REQUEST_ID_2).create());


        // assert both are sent on same connection
        assertEquals(1, channelPool.getConnectionIDs().size());

        // trigger write for both requests
        req1.get();
        req2.get();

        // wait for close message to arrive from server
        Thread.sleep(2000);

        // Assert that we should consider creating a connection only once, since only one connection is being closed.
        assertEquals(1, logCaptor.getLogs().stream().filter(str -> str.contains("Considering new connection on")).count());

        // assert sanity after connection replacement
        final Vertex v = client.submit("1",
                RequestOptions.build().overrideRequestId(settings.SINGLE_VERTEX_REQUEST_ID).create())
                .one().getVertex();
        assertNotNull(v);
    }

    /**
     * Tests the scenario when client intentionally closes the connection. In this case, the
     * connection should not be recycled.
     */
    @Test
    public void shouldNotCreateReplacementConnectionWhenClientClosesConnection() throws ExecutionException, InterruptedException {
        final Cluster cluster = Cluster.build("localhost").port(settings.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHSON_V2)
                .create();
        final Client.ClusteredClient client = cluster.connect();

        // Initialize the client preemptively
        client.init();

        // Clearing logCaptor before attempting to close the connection is in response to an issue where this test can
        // be polluted by logs from a previous test when running on slow hardware.
        logCaptor.clearLogs();

        // assert number of connections opened
        final ConnectionPool channelPool = client.hostConnectionPools.values().stream().findFirst().get();
        assertEquals(1, channelPool.getConnectionIDs().size());

        // close the connection pool in an authentic manner
        channelPool.closeAsync().get();

        // wait for channel closure callback to trigger
        Thread.sleep(2000);

        assertEquals("OnClose callback should be called but only once", 1,
                logCaptor.getLogs().stream().filter(str -> str.contains("OnChannelClose callback called for channel")).count());

        assertEquals("No new connection creation should be started", 0,
                logCaptor.getLogs().stream().filter(str -> str.contains("Considering new connection on")).count());
    }

    /**
     * (TINKERPOP-2814) Tests to make sure that the SSL handshake is now capped by connectionSetupTimeoutMillis and not
     * the default Netty SSL handshake timeout of 10,000ms.
     */
    @Test
    public void shouldAttemptHandshakeForLongerThanDefaultNettySslHandshakeTimeout() {
        final Cluster cluster = Cluster.build("localhost").port(settings.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .connectionSetupTimeoutMillis(20000) // needs to be larger than 10000ms.
                .enableSsl(true)
                .create();

        final Client.ClusteredClient client = cluster.connect();
        final long start = System.currentTimeMillis();

        Exception caught = null;
        try {
            client.submit("1");
        } catch (Exception e) {
            caught = e;
        } finally {
            // Test against 15000ms which should give a big enough buffer to avoid timing issues.
            assertTrue(System.currentTimeMillis() - start > 15000);
            assertTrue(caught != null);
            assertTrue(caught instanceof NoHostAvailableException);
            assertTrue(logCaptor.getLogs().stream().anyMatch(str -> str.contains("SSL handshake not completed")));
        }

        cluster.close();
    }

    /**
     * Tests to make sure that the correct error message is logged when a non-SSL connection attempt times out.
     */
    @Test
    public void shouldPrintCorrectErrorForRegularWebSocketHandshakeTimeout() throws InterruptedException {
        final Cluster cluster = Cluster.build("localhost").port(settings.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .connectionSetupTimeoutMillis(120)
                .create();

        final Client.ClusteredClient client = cluster.connect();

        Exception caught = null;
        try {
            client.submit("1");
        } catch (Exception e) {
            caught = e;
        } finally {
            assertTrue(caught != null);
            assertTrue(caught instanceof NoHostAvailableException);
            Thread.sleep(150);
            assertTrue(logCaptor.getLogs().stream().anyMatch(str -> str.contains("WebSocket handshake not completed")));
        }

        cluster.close();
    }

    /**
     * Tests that if a server throttles new connections (doesn't allow new connections to be made) then all requests
     * will run and complete on the connections that are already open.
     */
    @Test
    public void shouldContinueRunningRemainingConnectionsIfServerThrottlesNewConnections() throws ExecutionException, InterruptedException, TimeoutException {
        final Cluster cluster = Cluster.build("localhost").port(settings.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(5)
                .maxWaitForConnection(15000) // large value ensures that request will eventually find a connection.
                .connectionSetupTimeoutMillis(1000)
                .minInProcessPerConnection(0)
                .maxInProcessPerConnection(1)
                .minSimultaneousUsagePerConnection(0)
                .maxSimultaneousUsagePerConnection(1)
                .serializer(Serializers.GRAPHSON_V2)
                .create();

        final Client.ClusteredClient client = cluster.connect();

        final List<CompletableFuture<ResultSet>> results = new ArrayList<CompletableFuture<ResultSet>>();
        for (int i = 0; i < 5; i++) {
            results.add(client.submitAsync("500"));
        }

        for (CompletableFuture<ResultSet> result : results) {
            assertNotNull(result.get(60000, TimeUnit.MILLISECONDS).one().getVertex());
        }

        cluster.close();
    }

    /**
     * Tests that if a server throttles new connections (doesn't allow new connections to be made) then any request
     * that can't find a connection within its maxWaitForConnection will return an informative exception regarding
     * the inability to open new connections.
     */
    @Test
    public void shouldReturnCorrectExceptionIfServerThrottlesNewConnectionsAndMaxWaitExceeded() {
        final Cluster cluster = Cluster.build("localhost").port(settings.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(5)
                .maxWaitForConnection(250) // small value ensures that requests will return TimeoutException.
                .connectionSetupTimeoutMillis(100)
                .minInProcessPerConnection(0)
                .maxInProcessPerConnection(1)
                .minSimultaneousUsagePerConnection(0)
                .maxSimultaneousUsagePerConnection(1)
                .serializer(Serializers.GRAPHSON_V2)
                .create();

        final Client.ClusteredClient client = cluster.connect();

        for (int i = 0; i < 5; i++) {
            try {
                client.submitAsync("3000");
            } catch (Exception e) {
                final Throwable rootCause = ExceptionHelper.getRootCause(e);
                assertTrue(rootCause instanceof TimeoutException);
                assertTrue(rootCause.getMessage().contains("WebSocket handshake not completed"));
            }
        }

        cluster.close();
    }

    /**
     * Tests that the client continues to work if the server temporarily goes down between two requests.
     */
    @Test
    public void shouldContinueRunningIfServerGoesDownTemporarily() throws InterruptedException {
        final Cluster cluster = Cluster.build("localhost").port(settings.PORT)
                .minConnectionPoolSize(1)
                .serializer(Serializers.GRAPHSON_V2)
                .create();

        final Client.ClusteredClient client = cluster.connect();
        final Object lock = new Object();

        final ScheduledExecutorService scheduledPool = Executors.newScheduledThreadPool(1);
        scheduledPool.schedule(() -> {
            try {
                server.stopSync();
                server = new SimpleSocketServer(settings);
                server.start(new TestWSGremlinInitializer(settings));
                synchronized (lock) {
                    lock.notify();
                }
            } catch (InterruptedException ignored) {
                // Ignored.
            }
        }, 1000, TimeUnit.MILLISECONDS);

        synchronized (lock) {
            assertNotNull(client.submit("1").one().getVertex());
            lock.wait(30000);
        }

        assertNotNull(client.submit("1").one().getVertex());

        cluster.close();
    }

    /**
     * Tests that if the host is unavailable then the client will return an exception that contains information about
     * the status of the host.
     */
    @Test
    public void shouldReturnCorrectExceptionIfServerGoesDown() throws InterruptedException {
        final Cluster cluster = Cluster.build("localhost").port(settings.PORT)
                .minConnectionPoolSize(1)
                .maxWaitForConnection(500)
                .connectionSetupTimeoutMillis(100)
                .serializer(Serializers.GRAPHSON_V2)
                .create();

        final Client.ClusteredClient client = cluster.connect();
        client.submit("1");

        server.stopSync();

        try {
            client.submit("1");
        } catch (Exception e) {
            final Throwable rootCause = ExceptionHelper.getRootCause(e);
            assertTrue(rootCause instanceof TimeoutException);
            assertTrue(rootCause.getMessage().contains("Connection refused"));
        }

        cluster.close();
    }

    /**
     * Tests that client is correctly sending all overridable per request settings (requestId, batchSize,
     * evaluationTimeout, and userAgent) to the server.
     */
    @Test
    public void shouldSendPerRequestSettingsToServer() {
        final Cluster cluster = Cluster.build("localhost").port(settings.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHSON_V2)
                .create();
        final Client.ClusteredClient client = cluster.connect();

        // trigger the testing server to return captured request settings
        String response = client.submit("1", RequestOptions.build()
                .overrideRequestId(settings.PER_REQUEST_SETTINGS_REQUEST_ID)
                .timeout(1234).userAgent("helloWorld").batchSize(12)
                .materializeProperties("tokens").create()).one().getString();

        String expectedResponse = String.format("requestId=%s evaluationTimeout=%d, batchSize=%d, userAgent=%s, materializeProperties=%s",
                settings.PER_REQUEST_SETTINGS_REQUEST_ID, 1234, 12, "helloWorld", "tokens");
        assertEquals(expectedResponse, response);
    }
}
