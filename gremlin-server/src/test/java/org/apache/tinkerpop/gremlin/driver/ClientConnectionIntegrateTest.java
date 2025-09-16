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
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
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

import javax.net.ssl.SSLHandshakeException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
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
                                                 .serializer(Serializers.GRAPHBINARY_V1)
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
                .maxConnectionPoolSize(operations / usagePerConnection)
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

    /**
     * Added for TINKERPOP-2813 - this scenario would have previously thrown tons of
     * {@link NoHostAvailableException}.
     */
    @Test
    public void shouldSucceedWithJitteryConnection() throws Exception {
        final Cluster cluster = TestClientFactory.build().minConnectionPoolSize(1).maxConnectionPoolSize(4).
                reconnectInterval(1000).
                maxWaitForConnection(4000).validationRequest("g.inject()").create();
        final Client.ClusteredClient client = cluster.connect();

        client.init();

        // every 10 connections let's have some problems
        final JitteryConnectionFactory connectionFactory = new JitteryConnectionFactory(3);
        client.hostConnectionPools.forEach((h, pool) -> pool.connectionFactory = connectionFactory);

        // get an initial connection which marks the host as available
        assertEquals(2, client.submit("1+1").all().join().get(0).getInt());

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
                    client.submitAsync("1 + " + i);
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

    /**
     * Added for TINKERPOP-3181 - this scenario would have previously closed the connection pool and left us with
     * {@link NoHostAvailableException} just because a single connection failed on {@link Client} initialization while
     * others succeeded.
     */
    @Test
    public void shouldSucceedWithSingleConnectionFailureOnInit() throws Exception {
        // set the min connection pool size to 4 so that they all get created on init as that's the area we want to
        // test host availability behavior
        final Cluster cluster = TestClientFactory.build().minConnectionPoolSize(4).maxConnectionPoolSize(4).
                reconnectInterval(1000).
                maxWaitForConnection(4000).validationRequest("g.inject()").create();
        final Client.ClusteredClient client = cluster.connect();

        // we let 3 connections succeed but then fail on the 4th
        final SingleFailConnectionFactory connectionFactory = new SingleFailConnectionFactory(3);
        client.connectionFactorySupplier = () -> connectionFactory;

        // prior to 3.7.5, we would have seen this pop an exception and close the pool with error like:
        // Could not initialize 4 (minPoolSize) connections in pool. Successful connections=3. Closing the connection pool.
        // which doesn't really make sense because 3 prior connection were good. perhaps this fourth one just had a
        // very temporary network issue. the other 3 could technically still be serviceable. the 4th shouldn't end
        // connectivity and assume the host is dead as ultimately closing the pool at this point in init will end in
        // NoHostAvailableException.
        //
        // Starting at 3.7.5, we can allow a bit more failure here before killing the pool for just a single connection
        // failure. we might be below min pool size at init but we log that warning and expect fast recovery from the
        // driver in the best case and in the worst case, normal processes of reconnect kick in and stabilize.
        client.init();

        assertEquals(3, connectionFactory.getConnectionsCreated());

        // load up a hella ton of requests
        final int requests = 1000;
        final CountDownLatch latch = new CountDownLatch(requests);
        final AtomicBoolean hadFail = new AtomicBoolean(false);

        new Thread(() -> {
            IntStream.range(0, requests).forEach(i -> {
                try {
                    client.submitAsync("1 + " + i);
                } catch (Exception ex) {
                    // we could catch a TimeoutException here in some cases if the jitters cause a borrow of a
                    // connection to take too long. submitAsync() will wrap in a RuntimeException. can't assert
                    // this condition inside this thread or the test locks up
                    hadFail.compareAndSet(false, true);
                } finally {
                    latch.countDown();
                }
            });
        }, "worker-shouldSucceedWithSingleConnectionFailureOnInit").start();

        // wait for requests to complete
        assertTrue(latch.await(30000, TimeUnit.MILLISECONDS));

        // we can send some requests because we have 3 created connections
        assertEquals(2, client.submit("1+1").all().join().get(0).getInt());

        // not expecting any failures
        assertThat(hadFail.get(), is(false));

        cluster.close();
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

    /**
     * Introduces a failure after the specified number of {@link Connection} instance are created for the
     * {@link ConnectionPool}.
     */
    public static class SingleFailConnectionFactory implements ConnectionFactory {

        private int connectionsCreated = 0;
        private int failAfter;
        private boolean failedOnce = false;

        public SingleFailConnectionFactory(final int failAfter) {
            this.failAfter = failAfter;
        }

        public int getConnectionsCreated() {
            return connectionsCreated;
        }

        @Override
        public Connection create(final ConnectionPool pool) {
            if (!failedOnce && connectionsCreated == failAfter) {
                failedOnce = true;
                throw new ConnectionException(pool.host.getHostUri(),
                        new SSLHandshakeException("We big mad on purpose"));
            }
            connectionsCreated++;

            return ConnectionFactory.super.create(pool);
        }
    }
}
