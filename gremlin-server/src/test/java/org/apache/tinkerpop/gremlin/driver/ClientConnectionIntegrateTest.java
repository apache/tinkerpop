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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.netty.handler.codec.CorruptedFrameException;
import nl.altindag.log.LogCaptor;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.server.AbstractGremlinServerIntegrationTest;
import org.apache.tinkerpop.gremlin.server.TestClientFactory;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class ClientConnectionIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private static LogCaptor logCaptor;
    private Level previousLevel;

    @BeforeClass
    public static void setupLogCaptor() {
        logCaptor = LogCaptor.forClass(Connection.class);
    }

    @AfterClass
    public static void tearDownAfterClass() {
        logCaptor.close();
    }

    @Before
    public void setupForEachTest() {
        final Logger lc = (Logger) LoggerFactory.getLogger(Connection.class);
        previousLevel = lc.getLevel();
        lc.setLevel(Level.DEBUG);

        logCaptor.clearLogs();
    }

    @After
    public void afterEachTest() {
        final Logger lc = (Logger) LoggerFactory.getLogger(Connection.class);
        lc.setLevel(previousLevel);
    }

    /**
     * Reproducer for TINKERPOP-2169
     */
    @Test
    public void shouldCloseConnectionDeadDueToUnRecoverableError() throws Exception {
        // Set a low value of maxContentLength to intentionally trigger CorruptedFrameException
        final Cluster cluster = TestClientFactory.build()
                                                 .serializer(Serializers.GRAPHBINARY_V1D0)
                                                 .maxContentLength(64)
                                                 .minConnectionPoolSize(1)
                                                 .maxConnectionPoolSize(2)
                                                 .create();
        final Client.ClusteredClient client = cluster.connect();

        try {
            // Add the test data so that the g.V() response could exceed maxContentLength
            client.submit("g.inject(1).repeat(__.addV()).times(20).count()").all().get();
            try {
                client.submit("g.V().fold()").all().get();

                fail("Should throw an exception.");
            } catch (Exception re) {
                assertThat(re.getCause() instanceof CorruptedFrameException, is(true));
            }

            // without this wait this test is failing randomly on docker/travis with ConcurrentModificationException
            // see TINKERPOP-2504 - adjust the sleep to account for the max time to wait for sessions to close in
            // an orderly fashion
            Thread.sleep(Connection.MAX_WAIT_FOR_CLOSE + 1000);

            // Assert that the host has not been marked unavailable
            assertEquals(1, cluster.availableHosts().size());

            // Assert that there is no connection leak and all connections have been closed
            assertEquals(0, client.hostConnectionPools.values().stream()
                                                             .findFirst().get()
                                                             .numConnectionsWaitingToCleanup());
        } finally {
            cluster.close();
        }

        // Assert that the connection has been destroyed. Specifically check for the string with
        // isDead=true indicating the connection that was closed due to CorruptedFrameException.
        assertThat(logCaptor.getLogs().stream().anyMatch(m -> m.matches(
                "^(?!.*(isDead=false)).*isDead=true.*destroyed successfully.$")), Is.is(true));

    }

    @Test
    public void shouldBalanceConcurrentRequestsAcrossConnections() throws InterruptedException {
        final int connPoolSize = 16;
        final Cluster cluster = TestClientFactory.build()
                .minConnectionPoolSize(connPoolSize)
                .maxConnectionPoolSize(connPoolSize)
                .create();
        final Client.ClusteredClient client = cluster.connect();
        client.init();
        final ExecutorService executorServiceForTesting = cluster.executor();

        try {
            final RequestMessage.Builder request = client.buildMessage(RequestMessage.build(Tokens.OPS_EVAL))
                    .add(Tokens.ARGS_GREMLIN, "Thread.sleep(5000)");
            final Callable<Connection> sendQueryCallable = () -> client.chooseConnection(request.create());
            final List<Callable<Connection>> listOfTasks = new ArrayList<>();
            for (int i = 0; i < connPoolSize; i++) {
                listOfTasks.add(sendQueryCallable);
            }

            HashMap<String, Integer> channelsSize = new HashMap<>();

            final List<Future<Connection>> executorSubmitFutures = executorServiceForTesting.invokeAll(listOfTasks);
            executorSubmitFutures.parallelStream().map(fut -> {
                try {
                    return fut.get();
                } catch (InterruptedException | ExecutionException e) {
                    fail(e.getMessage());
                    return null;
                }
            }).forEach(conn -> {
                String id = conn.getChannelId();
                channelsSize.put(id, channelsSize.getOrDefault(id, 0) + 1);
            });

            assertNotEquals(channelsSize.entrySet().size(), 0);
            channelsSize.entrySet().forEach(entry -> {
                assertEquals(1, (entry.getValue()).intValue());
            });

        } finally {
            executorServiceForTesting.shutdown();
            client.close();
            cluster.close();
        }
    }

    @Test
    public void overLimitOperationsShouldDelegateToSingleNewConnection() throws InterruptedException {
        final int operations = 6;
        final int usagePerConnection = 3;
        final Cluster cluster = TestClientFactory.build()
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(operations)
                .minSimultaneousUsagePerConnection(1)
                .maxSimultaneousUsagePerConnection(usagePerConnection)
                .create();
        final Client.ClusteredClient client = cluster.connect();
        client.init();
        final ExecutorService executorServiceForTesting = cluster.executor();

        try {
            final RequestMessage.Builder request = client.buildMessage(RequestMessage.build(Tokens.OPS_EVAL))
                    .add(Tokens.ARGS_GREMLIN, "Thread.sleep(5000)");
            final Callable<Connection> sendQueryCallable = () -> client.chooseConnection(request.create());
            final List<Callable<Connection>> listOfTasks = new ArrayList<>();
            for (int i = 0; i < operations; i++) {
                listOfTasks.add(sendQueryCallable);
            }

            HashMap<String, Integer> connectionBorrowCount = new HashMap<>();

            final List<Future<Connection>> executorSubmitFutures = executorServiceForTesting.invokeAll(listOfTasks);
            executorSubmitFutures.parallelStream().map(fut -> {
                try {
                    return fut.get();
                } catch (InterruptedException | ExecutionException e) {
                    fail(e.getMessage());
                    return null;
                }
            }).forEach(conn -> {
                synchronized (this) {
                    String id = conn.getChannelId();
                    connectionBorrowCount.put(id, connectionBorrowCount.getOrDefault(id, 0) + 1);
                }
            });

            assertEquals(2, connectionBorrowCount.size());
            for (int finalBorrowCount : connectionBorrowCount.values()) {
                assertEquals(usagePerConnection, finalBorrowCount);
            }

        } finally {
            executorServiceForTesting.shutdown();
            client.close();
            cluster.close();
        }
    }
}
