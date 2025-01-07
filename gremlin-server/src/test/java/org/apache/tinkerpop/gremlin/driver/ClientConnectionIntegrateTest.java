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
import io.netty.handler.codec.TooLongFrameException;
import nl.altindag.log.LogCaptor;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.server.AbstractGremlinServerIntegrationTest;
import org.apache.tinkerpop.gremlin.server.TestClientFactory;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.Request;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.net.ssl.SSLHandshakeException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.*;

public class ClientConnectionIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private static LogCaptor logCaptor;
    private Level previousConnectionLevel;
    private Level previousConnectionPoolLevel;

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
        previousConnectionLevel = lc.getLevel();
        lc.setLevel(Level.DEBUG);
        final Logger lcp = (Logger) LoggerFactory.getLogger(ConnectionPool.class);
        previousConnectionPoolLevel = lcp.getLevel();
        lcp.setLevel(Level.DEBUG);

        logCaptor.clearLogs();
    }

    @After
    public void afterEachTest() {
        final Logger lc = (Logger) LoggerFactory.getLogger(Connection.class);
        lc.setLevel(previousConnectionLevel);
        final Logger lcp = (Logger) LoggerFactory.getLogger(ConnectionPool.class);
        lcp.setLevel(previousConnectionPoolLevel);
    }

    /**
     * Reproducer for TINKERPOP-2169
     */
    @Test
    public void shouldCloseConnectionDeadDueToUnRecoverableError() throws Exception {
        // Set a low value of maxResponseContentLength to intentionally trigger CorruptedFrameException
        final Cluster cluster = TestClientFactory.build()
                                                 .maxResponseContentLength(64)
                                                 .maxConnectionPoolSize(2)
                                                 .create();
        final Client.ClusteredClient client = cluster.connect();

        try {
            // Add the test data so that the g.V() response could exceed maxResponseContentLength
            client.submit("g.inject(1).repeat(__.addV()).times(20).count()").all().get();
            try {
                client.submit("g.V().fold()").all().get();

                fail("Should throw an exception.");
            } catch (Exception re) {
                assertThat(re.getCause() instanceof TooLongFrameException, is(true));
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
                .maxConnectionPoolSize(connPoolSize)
                .create();
        final Client.ClusteredClient client = cluster.connect();
        client.init();
        final ExecutorService executorServiceForTesting = cluster.executor();

        try {
            final RequestMessage.Builder request = client.buildMessage(RequestMessage.build("Thread.sleep(5000)"));
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
    public void shouldCreateNewHttpConnectionPerRequestAsNeeded() throws InterruptedException {
        final int operations = 6;
        final Cluster cluster = TestClientFactory.build()
                .maxConnectionPoolSize(operations)
                .create();
        final Client.ClusteredClient client = cluster.connect();
        client.init();
        final ExecutorService executorServiceForTesting = cluster.executor();

        try {
            final RequestMessage.Builder request = client.buildMessage(RequestMessage.build("Thread.sleep(5000)"));
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

            assertEquals(operations, connectionBorrowCount.size());
            for (int finalBorrowCount : connectionBorrowCount.values()) {
                assertEquals(1, finalBorrowCount);
            }

        } finally {
            executorServiceForTesting.shutdown();
            client.close();
            cluster.close();
        }
    }

    /**
     * Added for TINKERPOP-2813 - this scenario would have previously thrown tons of
     * {@link NoHostAvailableException}.
     */
    @Test
    public void shouldSucceedWithJitteryConnection() throws Exception {
        final Cluster cluster = TestClientFactory.build().maxConnectionPoolSize(128).
                reconnectInterval(1000).
                maxWaitForConnection(4000).validationRequest("g.inject()").create();
        final Client.ClusteredClient client = cluster.connect();

        client.init();

        // every 10 connections let's have some problems
        final JitteryConnectionFactory connectionFactory = new JitteryConnectionFactory(3);
        client.hostConnectionPools.forEach((h, pool) -> pool.connectionFactory = connectionFactory);

        // get an initial connection which marks the host as available
        assertEquals(2, client.submit("g.inject(2)").all().join().get(0).getInt());

        // network is gonna get fishy - ConnectionPool should try to grow during the workload below and when it
        // does some connections will fail to create in the background which should log some errors but not tank
        // the submit() as connections that are currently still working and active should be able to handle the load.
        connectionFactory.jittery = true;

        // load up a hella ton of requests
        final int requests = 1000;
        final CountDownLatch latch = new CountDownLatch(requests);
        final AtomicBoolean hadFailOtherThanTimeout = new AtomicBoolean(false);

        new Thread(() -> {
            IntStream.range(0, requests).forEach(i -> {
                try {
                    client.submitAsync(String.format("g.inject(%d)", 1+i));
                } catch (Exception ex) {
                    // we could catch a TimeoutException here in some cases if the jitters cause a borrow of a
                    // connection to take too long. submitAsync() will wrap in a RuntimeException. can't assert
                    // this condition inside this thread or the test locks up
                    hadFailOtherThanTimeout.compareAndSet(false, !(ex.getCause() instanceof TimeoutException));
                } finally {
                    latch.countDown();
                }
            });
        }, "worker-shouldSucceedWithJitteryConnection").start();

        // wait long enough for the jitters to kick in at least a little
        while (latch.getCount() > (requests / 2)) {
            TimeUnit.MILLISECONDS.sleep(50);
        }

        // wait for requests to complete
        assertTrue(latch.await(30000, TimeUnit.MILLISECONDS));

        // make sure we had some failures for sure coming out the factory
        assertThat(connectionFactory.getNumberOfFailures(), is(greaterThan(0L)));

        // if there was a exception in the worker thread, then it had better be a TimeoutException
        assertThat(hadFailOtherThanTimeout.get(), is(false));

        cluster.close();
    }

    @Test
    public void shouldCloseIdleConnectionsAndRecreateNewConnections() throws InterruptedException {
        int idleMillis = 3000;
        final Cluster cluster = TestClientFactory.build()
                .idleConnectionTimeoutMillis(idleMillis)
                .create();
        final Client.ClusteredClient client = cluster.connect();
        client.init();
        final ExecutorService executorServiceForTesting = cluster.executor();

        try {
            // create or reuse some connections
            chooseConnections(3, client, executorServiceForTesting);

            // simulate downtime where there is no traffic so that all connections become idle
            TimeUnit.MILLISECONDS.sleep(idleMillis * 3);
            assertEquals(1, client.hostConnectionPools.size());
            // all connections should have been closed due to idle timeout
            assertTrue(client.hostConnectionPools.values().iterator().next().getPoolInfo().contains("no connections in pool"));

            // create or reuse some more connections
            chooseConnections(4, client, executorServiceForTesting);

        } finally {
            executorServiceForTesting.shutdown();
            client.close();
            cluster.close();
        }
    }

    @Test
    public void shouldThrowErrorWithHelpfulMessageWhenIdleTimeoutReachedBeforeResponseReceived() throws InterruptedException {
        int idleMillis = 1000;
        final Cluster cluster = TestClientFactory.build()
                .idleConnectionTimeoutMillis(idleMillis)
                .create();
        final Client.ClusteredClient client = cluster.connect();
        final RequestOptions ro = RequestOptions.build().language("gremlin-groovy").create();

        try {
            client.submit("Thread.sleep(" + idleMillis * 3 + ")", ro).all().get();
            fail("Expected exception due to idle timeout");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Idle timeout occurred before response could be received from server - consider increasing idleConnectionTimeout"));
        } finally {
            client.close();
            cluster.close();
        }
    }

    private static void chooseConnections(int operations, Client.ClusteredClient client, ExecutorService executorServiceForTesting) throws InterruptedException {
        final RequestMessage.Builder request = client.buildMessage(RequestMessage.build("g.inject(1)"));
        final Callable<Connection> sendQueryCallable = () -> client.chooseConnection(request.create());
        final List<Callable<Connection>> listOfTasks = new ArrayList<>();
        for (int i = 0; i < operations; i++) {
            listOfTasks.add(sendQueryCallable);
        }

        final List<Future<Connection>> executorSubmitFutures = executorServiceForTesting.invokeAll(listOfTasks);
        executorSubmitFutures.parallelStream().map(fut -> {
            try {
                return fut.get();
            } catch (InterruptedException | ExecutionException e) {
                fail(e.getMessage());
                return null;
            }
        }).forEach(Assert::assertNotNull);
    }

    /**
     * Introduces random failures when creating a {@link Connection} for the {@link ConnectionPool}.
     */
    public static class JitteryConnectionFactory implements ConnectionFactory {

        private volatile boolean jittery = false;
        private final AtomicLong connectionsCreated = new AtomicLong(0);
        private final AtomicLong connectionFailures = new AtomicLong(0);
        private final int numberOfConnectionsBetweenErrors;

        public JitteryConnectionFactory(final int numberOfConnectionsBetweenErrors) {
            this.numberOfConnectionsBetweenErrors = numberOfConnectionsBetweenErrors;
        }

        public long getNumberOfFailures() {
            return connectionFailures.get();
        }

        @Override
        public Connection create(final ConnectionPool pool) {

            // fail creating a connection every 10 attempts or so when jittery
            if (jittery && connectionsCreated.incrementAndGet() % numberOfConnectionsBetweenErrors == 0) {
                connectionFailures.incrementAndGet();
                throw new ConnectionException(pool.host.getHostUri(),
                        new SSLHandshakeException("SSL on the funk - server is big mad with the jitters"));
            }

            return ConnectionFactory.super.create(pool);
        }
    }
}
