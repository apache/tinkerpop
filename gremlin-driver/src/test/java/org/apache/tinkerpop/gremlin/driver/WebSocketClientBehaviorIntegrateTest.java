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
import org.junit.Assert;
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

public class WebSocketClientBehaviorIntegrateTest {
    @Rule
    public TestName name = new TestName();

    private static final Logger logger = LoggerFactory.getLogger(WebSocketClientBehaviorIntegrateTest.class);
    private Log4jRecordingAppender recordingAppender = null;
    private Level previousLogLevel;
    private SimpleWebSocketServer server;

    @Before
    public void setUp() throws InterruptedException {
        recordingAppender = new Log4jRecordingAppender();
        final org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
        if (name.getMethodName().equals("TestClosedConnectionIsRecycledForConnectionWithPendingRequests")) {
            final org.apache.log4j.Logger connectionLogger = org.apache.log4j.Logger.getLogger(ConnectionPool.class);
            previousLogLevel = connectionLogger.getLevel();
            connectionLogger.setLevel(Level.DEBUG);
        }

        rootLogger.addAppender(recordingAppender);

        server = new SimpleWebSocketServer();
        server.start(new TestWSGremlinInitializer());
    }

    @After
    public void shutdown() {
        server.stop();

        // reset logger
        final org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();

        if (name.getMethodName().equals("TestClosedConnectionIsRecycledForConnectionWithPendingRequests")) {
            final org.apache.log4j.Logger connectionLogger = org.apache.log4j.Logger.getLogger(Connection.class);
            connectionLogger.setLevel(previousLogLevel);
        }

        rootLogger.removeAppender(recordingAppender);
    }

    /**
     * Test a scenario when server closes a connection which does not have any active requests. Such connection
     * should be recycled and replaced by another connection.
     */
    @Test
    public void TestClosedConnectionIsRecycledForConnectionWithNoRequests() throws InterruptedException {
        final Cluster cluster = Cluster.build("localhost").port(SimpleWebSocketServer.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHSON_V2D0)
                .create();
        final Client.ClusteredClient client = cluster.connect();

        // Initialize the client preemptively
        client.init();

        // assert number of connections opened
        ConnectionPool channelPool = client.hostConnectionPools.values().stream().findFirst().get();
        Assert.assertEquals(1, channelPool.getConnectionIDs().size());

        final String originalConnectionID = channelPool.getConnectionIDs().iterator().next();
        logger.info("On client init ConnectionIDs: " + channelPool.getConnectionIDs());

        // trigger the testing server to send a WS close frame
        Vertex v = client.submit("1", RequestOptions.build()
                .overrideRequestId(TestWSGremlinInitializer.SINGLE_VERTEX_DELAYED_CLOSE_CONNECTION_REQUEST_ID).create())
                .one().getVertex();

        Assert.assertNotNull(v);

        // assert connection is not closed yet
        Assert.assertEquals(1, channelPool.getConnectionIDs().size());

        // wait for server to send the close WS frame
        Thread.sleep(6000);

        // assert that original connection is not part of the connection pool any more
        Assert.assertFalse("The original connection should have been closed by the server.",
                channelPool.getConnectionIDs().contains(originalConnectionID));

        // assert sanity after connection replacement
        v = client.submit("1",
                RequestOptions.build().overrideRequestId(TestWSGremlinInitializer.SINGLE_VERTEX_REQUEST_ID).create())
                .one().getVertex();
        Assert.assertNotNull(v);
    }

    /**
     * Tests a scenario when the connection a faulty connection replaced by a new connection.
     * Ensures that the creation of a new replacement channel only happens once.
     */
    @Test
    public void TestClosedConnectionIsRecycledForConnectionWithPendingRequests() throws InterruptedException, ExecutionException {
        final Cluster cluster = Cluster.build("localhost").port(SimpleWebSocketServer.PORT)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .serializer(Serializers.GRAPHSON_V2D0)
                .create();

        final Client.ClusteredClient client = cluster.connect();

        // Initialize the client preemptively
        client.init();

        // assert number of connections opened
        ConnectionPool channelPool = client.hostConnectionPools.values().stream().findFirst().get();
        Assert.assertEquals(1, channelPool.getConnectionIDs().size());

        // Send two requests in flight. Both should error out.
        CompletableFuture<ResultSet> req1 = client.submitAsync("1", RequestOptions.build()
                .overrideRequestId(TestWSGremlinInitializer.CLOSE_CONNECTION_REQUEST_ID).create());
        CompletableFuture<ResultSet> req2 = client.submitAsync("1", RequestOptions.build()
                .overrideRequestId(TestWSGremlinInitializer.CLOSE_CONNECTION_REQUEST_ID_2).create());


        // assert both are sent on same connection
        Assert.assertEquals(1, channelPool.getConnectionIDs().size());

        // trigger write for both requests
        req1.get();
        req2.get();

        // wait for close message to arrive from server
        Thread.sleep(2000);

        // Assert that we should consider creating a connection only once, since only one connection is being closed.
        Assert.assertEquals(1, recordingAppender.getMessages().stream().filter(str -> str.contains("Considering new connection on")).count());

        // assert sanity after connection replacement
        Vertex v = client.submit("1",
                RequestOptions.build().overrideRequestId(TestWSGremlinInitializer.SINGLE_VERTEX_REQUEST_ID).create())
                .one().getVertex();
        Assert.assertNotNull(v);
    }
}