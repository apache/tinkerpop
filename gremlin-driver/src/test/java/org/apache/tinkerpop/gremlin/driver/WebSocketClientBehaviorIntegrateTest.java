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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.log4j.Level;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.Log4jRecordingAppender;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class WebSocketClientBehaviorIntegrateTest {
    @Rule
    public TestName name = new TestName();

    private static final Logger logger = LoggerFactory.getLogger(WebSocketClientBehaviorIntegrateTest.class);
    private Log4jRecordingAppender recordingAppender = null;
    private Level previousLogLevel;
    private SimpleSocketServer server;

    @Before
    public void setUp() throws InterruptedException {
        recordingAppender = new Log4jRecordingAppender();
        final org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
        if (name.getMethodName().equals("shouldRemoveConnectionFromPoolWhenServerClose_WithPendingRequests") ||
                name.getMethodName().equals("shouldNotCreateReplacementConnectionWhenClientClosesConnection")) {
            final org.apache.log4j.Logger connectionPoolLogger = org.apache.log4j.Logger.getLogger(ConnectionPool.class);
            final org.apache.log4j.Logger connectionLogger = org.apache.log4j.Logger.getLogger(Connection.class);
            previousLogLevel = connectionPoolLogger.getLevel();
            connectionPoolLogger.setLevel(Level.DEBUG);
            connectionLogger.setLevel(Level.DEBUG);
        }

        rootLogger.addAppender(recordingAppender);

        server = new SimpleSocketServer();
        server.start(new TestWSGremlinInitializer());
    }

    @After
    public void shutdown() {
        server.stop();

        // reset logger
        final org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();

        if (name.getMethodName().equals("shouldRemoveConnectionFromPoolWhenServerClose_WithPendingRequests") ||
                name.getMethodName().equals("shouldNotCreateReplacementConnectionWhenClientClosesConnection")) {
            final org.apache.log4j.Logger connectionPoolLogger = org.apache.log4j.Logger.getLogger(ConnectionPool.class);
            final org.apache.log4j.Logger connectionLogger = org.apache.log4j.Logger.getLogger(Connection.class);
            connectionPoolLogger.setLevel(previousLogLevel);
            connectionLogger.setLevel(previousLogLevel);
        }

        rootLogger.removeAppender(recordingAppender);
    }

    /**
     * Constructs a deadlock situation when initializing a {@link Client} object in sessionless form that leads to
     * hanging behavior in low resource environments (TINKERPOP-2504) and for certain configurations of the
     * {@link Cluster} object where there are simply not enough threads to properly allow the {@link Host} and its
     * related {@link ConnectionPool} objects to spin up properly - see TINKERPOP-2550.
     */
    @Test
    public void shouldNotDeadlockOnInitialization() throws Exception {
        // it seems you cah add the same host more than once so while kinda weird it is helpful in faithfully
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
                addContactPoint("localhost").port(SimpleSocketServer.PORT).
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
        final Cluster cluster = Cluster.build("localhost").port(SimpleSocketServer.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHSON_V2D0)
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
                .overrideRequestId(TestWSGremlinInitializer.SINGLE_VERTEX_DELAYED_CLOSE_CONNECTION_REQUEST_ID).create())
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
                RequestOptions.build().overrideRequestId(TestWSGremlinInitializer.SINGLE_VERTEX_REQUEST_ID).create())
                .one().getVertex();
        assertNotNull(v);
    }

    /**
     * Tests a scenario when the connection a faulty connection replaced by a new connection.
     * Ensures that the creation of a new replacement channel only happens once.
     */
    @Test
    public void shouldRemoveConnectionFromPoolWhenServerClose_WithPendingRequests() throws InterruptedException, ExecutionException {
        final Cluster cluster = Cluster.build("localhost").port(SimpleSocketServer.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHSON_V2D0)
                .create();

        final Client.ClusteredClient client = cluster.connect();

        // Initialize the client preemptively
        client.init();

        // assert number of connections opened
        final ConnectionPool channelPool = client.hostConnectionPools.values().stream().findFirst().get();
        assertEquals(1, channelPool.getConnectionIDs().size());

        // Send two requests in flight. Both should error out.
        final CompletableFuture<ResultSet> req1 = client.submitAsync("1", RequestOptions.build()
                .overrideRequestId(TestWSGremlinInitializer.CLOSE_CONNECTION_REQUEST_ID).create());
        final CompletableFuture<ResultSet> req2 = client.submitAsync("1", RequestOptions.build()
                .overrideRequestId(TestWSGremlinInitializer.CLOSE_CONNECTION_REQUEST_ID_2).create());


        // assert both are sent on same connection
        assertEquals(1, channelPool.getConnectionIDs().size());

        // trigger write for both requests
        req1.get();
        req2.get();

        // wait for close message to arrive from server
        Thread.sleep(2000);

        // Assert that we should consider creating a connection only once, since only one connection is being closed.
        assertEquals(1, recordingAppender.getMessages().stream().filter(str -> str.contains("Considering new connection on")).count());

        // assert sanity after connection replacement
        final Vertex v = client.submit("1",
                RequestOptions.build().overrideRequestId(TestWSGremlinInitializer.SINGLE_VERTEX_REQUEST_ID).create())
                .one().getVertex();
        assertNotNull(v);
    }

    /**
     * Tests the scenario when client intentionally closes the connection. In this case, the
     * connection should not be recycled.
     */
    @Test
    public void shouldNotCreateReplacementConnectionWhenClientClosesConnection() throws ExecutionException, InterruptedException {
        final Cluster cluster = Cluster.build("localhost").port(SimpleSocketServer.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHSON_V2D0)
                .create();
        final Client.ClusteredClient client = cluster.connect();

        // Initialize the client preemptively
        client.init();

        // assert number of connections opened
        final ConnectionPool channelPool = client.hostConnectionPools.values().stream().findFirst().get();
        assertEquals(1, channelPool.getConnectionIDs().size());

        // close the connection pool in an authentic manner
        channelPool.closeAsync().get();

        // wait for channel closure callback to trigger
        Thread.sleep(2000);

        assertEquals("OnClose callback should be called but only once", 1,
                recordingAppender.getMessages().stream()
                        .filter(str -> str.contains("OnChannelClose callback called for channel"))
                        .count());

        assertEquals("No new connection creation should be started", 0,
                recordingAppender.getMessages().stream()
                        .filter(str -> str.contains("Considering new connection on"))
                        .count());
    }
}