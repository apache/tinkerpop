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

import io.netty.handler.codec.CorruptedFrameException;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.server.AbstractGremlinServerIntegrationTest;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.TestClientFactory;
import org.apache.tinkerpop.gremlin.util.Log4jRecordingAppender;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ClientSingleRequestConnectionIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private Log4jRecordingAppender recordingAppender = null;
    private Level previousLogLevel;

    @Before
    public void setupForEachTest() {
        recordingAppender = new Log4jRecordingAppender();
        final Logger rootLogger = Logger.getRootLogger();

        if (name.getMethodName().equals("shouldRecoverFromConnectionCloseDueToUnRecoverableError")) {
            final org.apache.log4j.Logger connectionLogger = org.apache.log4j.Logger.getLogger(SingleRequestConnection.class);
            previousLogLevel = connectionLogger.getLevel();
            connectionLogger.setLevel(Level.DEBUG);
        }

        if (name.getMethodName().equals("testConnectionReleaseOnResultSetClose")) {
            final org.apache.log4j.Logger connectionLogger = org.apache.log4j.Logger.getLogger(SingleRequestConnection.class);
            previousLogLevel = connectionLogger.getLevel();
            connectionLogger.setLevel(Level.DEBUG);
        }

        if (name.getMethodName().equals("testGracefulClose") || name.getMethodName().equals("testAbruptClose")) {
            final org.apache.log4j.Logger connectionPoolLogger = org.apache.log4j.Logger.getLogger(ConnectionPoolImpl.class);
            previousLogLevel = connectionPoolLogger.getLevel();
            connectionPoolLogger.setLevel(Level.INFO);
        }

        rootLogger.addAppender(recordingAppender);
    }

    @After
    public void teardownForEachTest() {
        final Logger rootLogger = Logger.getRootLogger();

        if (name.getMethodName().equals("shouldRecoverFromConnectionCloseDueToUnRecoverableError")) {
            final org.apache.log4j.Logger connectionLogger = org.apache.log4j.Logger.getLogger(SingleRequestConnection.class);
            connectionLogger.setLevel(previousLogLevel);
        }

        if (name.getMethodName().equals("testConnectionReleaseOnResultSetClose")) {
            final org.apache.log4j.Logger connectionLogger = org.apache.log4j.Logger.getLogger(SingleRequestConnection.class);
            connectionLogger.setLevel(previousLogLevel);
        }

        if (name.getMethodName().equals("testGracefulClose") || name.getMethodName().equals("testAbruptClose")) {
            final org.apache.log4j.Logger connectionLogger = org.apache.log4j.Logger.getLogger(ConnectionPoolImpl.class);
            connectionLogger.setLevel(previousLogLevel);
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
            case "testClientCloseInMiddleOfResultConsumption":
                settings.writeBufferHighWaterMark = 32;
                settings.writeBufferLowWaterMark = 16;
                break;
        }
        return settings;
    }

    /**
     * Netty would close the channel on an un-recoverable exception such as CorruptedFrameException.
     * This test validates the correct replacement of the channel.
     */
    @Test
    public void shouldRecoverFromConnectionCloseDueToUnRecoverableError() throws Exception {
        // Set a low value of maxContentLength to intentionally trigger CorruptedFrameException
        final Cluster cluster = TestClientFactory.build()
                                                 .maxContentLength(96)
                                                 .maxConnectionPoolSize(1)
                                                 .maxSimultaneousUsagePerConnection(0)
                                                 .minSimultaneousUsagePerConnection(0)
                                                 .maxInProcessPerConnection(0)
                                                 .minInProcessPerConnection(0)
                                                 .create();
        final Client.ClusteredClient client = cluster.connect();

        try {
            // Add the test data so that the g.V() response could exceed maxContentLength
            client.submit("g.inject(1).repeat(__.addV()).times(10).count()").all().get();

            try {
                client.submit("g.V().fold()").all().get();
                fail("Should throw an exception.");
            } catch (Exception re) {
                assertThat(re.getCause() instanceof CorruptedFrameException, is(true));
            }

            Thread.sleep(2000);

            // Assert that the host has not been marked unavailable
            assertEquals(1, cluster.availableHosts().size());

            assertThat("Unable to find statement in the log.",
                       this.recordingAppender.logContainsAny(".*Error while reading the response from the server.*"), is(true));

            // Assert that we are able to send requests from the pool (it should have replaced the connection)
            client.submit("g.V().limit(1).id()").all().get();
        } finally {
            cluster.close();
        }
    }

    @Test
    public void testReuseSameConnection() throws Exception {
        final Cluster cluster = this.createClusterWithXNumOfConnection(1);
        final Client.ClusteredClient client = cluster.connect();

        try {
            ResultSet resultSet = client.submit("g.V().limit(1)");
            final String channelIdForFirstRequest = resultSet.getChannelId();

            Assert.assertTrue(StringUtils.isNotBlank(channelIdForFirstRequest));
            resultSet.all().get();

            resultSet = client.submit("g.V().limit(1)");

            final String channelIdForSecondRequest = resultSet.getChannelId();

            Assert.assertTrue(StringUtils.isNotBlank(channelIdForSecondRequest));
            resultSet.all().get();

            Assert.assertEquals(channelIdForFirstRequest, channelIdForSecondRequest);
        } finally {
            cluster.close();
        }
    }

    @Test
    public void testTimeoutOnExpiredMaxWaitForConnection() {
        final Cluster cluster = TestClientFactory.build()
                                                 .maxConnectionPoolSize(2)
                                                 .maxSimultaneousUsagePerConnection(0)
                                                 .minSimultaneousUsagePerConnection(0)
                                                 .maxInProcessPerConnection(0)
                                                 .minInProcessPerConnection(0)
                                                 .maxWaitForConnection(500)
                                                 .create();

        final Client.ClusteredClient client = cluster.connect();

        try {
            CompletableFuture<ResultSet> rs1 = client.submitAsync("Thread.sleep(3000);'done'");
            CompletableFuture<ResultSet> rs2 = client.submitAsync("Thread.sleep(3000);'done'");

            CompletableFuture.allOf(rs1, rs2).join();

            client.submit("g.V().limit(1)");

            fail("Should throw exception");
        } catch (Exception ex) {
            assertThat("Should throw timeout exception on max wait expired",
                       ex.getCause().getCause() instanceof TimeoutException, is(true));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailAttemptToSendRequestToConnectionInUse() {
        final Cluster cluster = this.createClusterWithXNumOfConnection(1);
        final Client.ClusteredClient client = cluster.connect();
        try {
            // Add some test data
            client.submit("g.inject(1).repeat(__.addV()).times(12).count()").all().get();

            final String query = "g.V()";
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                                                         .addArg(Tokens.ARGS_GREMLIN, query).create();
            final Connection conn = client.chooseConnectionAsync(request).get();

            final CompletableFuture<ResultSet> resultFuture = new CompletableFuture<>();
            conn.write(request, resultFuture).get();

            // Second request to the same connection should fail while the first is in progress
            conn.write(request, resultFuture);

            fail("Should throw exception");
        } catch (Exception ex) {
            assertThat("Should throw exception on trying to send another request on busy connection",
                       ex instanceof IllegalStateException, is(true));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void testClientWriteOnServerUnreachable() throws Exception {
        final Cluster cluster = this.createClusterWithXNumOfConnection(1);
        final Client.ClusteredClient client = cluster.connect();
        try {
            final int resultCountToGenerate = 1000;
            final String fatty = IntStream.range(0, 175).mapToObj(String::valueOf).collect(Collectors.joining());
            final String fattyX = "['" + fatty + "'] * " + resultCountToGenerate;

            final int batchSize = 2;
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                                                         .addArg(Tokens.ARGS_BATCH_SIZE, batchSize)
                                                         .addArg(Tokens.ARGS_GREMLIN, fattyX).create();

            client.init();
            final Connection conn = client.chooseConnectionAsync(request).get();

            // stop the server to mimic a situation where server goes down before establishing a connection
            this.stopServer();

            final CompletableFuture<ResultSet> resultFuture = new CompletableFuture<>();

            try {
                conn.write(request, resultFuture);
                resultFuture.join();
                fail("Should throw exception.");
            } catch (Exception ex) {
                assertThat("Should throw ConnectException on unreachable server",
                           ex.getCause() instanceof ConnectException, is(true));
            }

            this.startServer();

            // Verify that client has recovered from this error. Ideally the server should also have released
            // the resources associated with this request but this test doesn't verify that.
            client.submit("g.V().limit(1)").all().get();
        } finally {
            cluster.close();
        }
    }

    @Test
    public void testClientCloseInMiddleOfResultConsumption() throws Exception {
        final Cluster cluster = this.createClusterWithXNumOfConnection(1);
        final Client.ClusteredClient client = cluster.connect();
        try {
            final int resultCountToGenerate = 1000;
            final String fatty = IntStream.range(0, 175).mapToObj(String::valueOf).collect(Collectors.joining());
            final String fattyX = "['" + fatty + "'] * " + resultCountToGenerate;

            final int batchSize = 2;
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                                                         .addArg(Tokens.ARGS_BATCH_SIZE, batchSize)
                                                         .addArg(Tokens.ARGS_GREMLIN, fattyX).create();

            client.init();
            Connection conn = client.chooseConnectionAsync(request).get();
            final CompletableFuture<ResultSet> resultFuture = new CompletableFuture<>();
            conn.write(request, resultFuture).syncUninterruptibly().getNow();
            final ResultSet rs1 = resultFuture.get();
            final Result res = rs1.iterator().next();
            final String channelIdForFirstRequest = rs1.getChannelId();

            Assert.assertTrue(StringUtils.isNotBlank(channelIdForFirstRequest));

            // verify that the server is giving out some results
            Assert.assertEquals(fatty, res.getString());

            try {
                // return the connection to the pool while all results have not been read
                ((SingleRequestConnection) conn).releaseResources().get();
                resultFuture.get().one();
                fail("Should throw exception.");
            } catch (Exception ex) {
                assertThat("Should throw IllegalStateException on reading results from a connection that has been returned to the pool",
                           ex.getCause() instanceof IllegalStateException, is(true));
            }

            // Verify that client has recovered from this abrupt close. Ideally the server should also have released
            // the resources associated with this request but this test doesn't verify that.
            RequestMessage request2 = RequestMessage.build(Tokens.OPS_EVAL)
                                                    .addArg(Tokens.ARGS_BATCH_SIZE, batchSize)
                                                    .addArg(Tokens.ARGS_GREMLIN, "g.V().limit(1)").create();
            conn = client.chooseConnectionAsync(request2).get();
            final CompletableFuture<ResultSet> resultFuture2 = new CompletableFuture<>();
            conn.write(request2, resultFuture2).syncUninterruptibly().getNow();

            ResultSet rs2 = resultFuture2.get();
            // Verify that the same channel is re-used again
            final String channelIdForSecondRequest = rs2.getChannelId();
            Assert.assertTrue(StringUtils.isNotBlank(channelIdForSecondRequest));

            rs2.all().get();

            Assert.assertEquals(channelIdForFirstRequest, channelIdForSecondRequest);
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldPropagateLegitResponseExceptionFromServer() throws Exception {
        final Cluster cluster = this.createClusterWithXNumOfConnection(1);
        final Client.ClusteredClient client = cluster.connect();

        try {
            ResultSet resultSet = client.submit("g.V().X()");
            final String channelIdForFirstRequest = resultSet.getChannelId();

            Assert.assertTrue(StringUtils.isNotBlank(channelIdForFirstRequest));

            try {
                resultSet.all().get();
            } catch (Exception ex) {
                assertThat("Should throw ResponseException on genuine server errors.",
                           ex.getCause() instanceof ResponseException, is(true));
            }

            resultSet = client.submit("g.V().limit(1)");

            final String channelIdForSecondRequest = resultSet.getChannelId();

            Assert.assertTrue(StringUtils.isNotBlank(channelIdForSecondRequest));
            resultSet.all().get();

            Assert.assertEquals(channelIdForFirstRequest, channelIdForSecondRequest);

        } finally {
            cluster.close();
        }
    }

    @Test
    public void testGracefulClose() throws ExecutionException, InterruptedException, TimeoutException {
        // For this test to succeed ensure that server query timeout >
        // (number of requests/number of executor thread in server)*wait per request
        final Cluster cluster = this.createClusterWithXNumOfConnection(250);

        try {
            final Client.ClusteredClient client = cluster.connect();

            final int requests = 250;
            final List<CompletableFuture<ResultSet>> futures = new ArrayList<>(requests);
            IntStream.range(0, requests).forEach(i -> {
                try {
                    futures.add(client.submitAsync("Thread.sleep(100);"));
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });

            assertEquals(requests, futures.size());

            int counter = 0;
            for (CompletableFuture<ResultSet> f : futures) {
                f.get().all().get(30000, TimeUnit.MILLISECONDS);
                counter++;
            }

            assertEquals(requests, counter);
        } finally {
            cluster.close();
        }

        assertThat(recordingAppender.logContainsAny("INFO - Closed ConnectionPool\\{closing=true, host=Host\\{address=.*, hostUri=.*\\}, BusyConnectionCount=0\\}"), is(true));
        // No errors or warnings should be printed
        assertThat(recordingAppender.logContainsAny("ERROR - .*"), is(false));
    }

    @Test
    public void testAbruptClose() throws ExecutionException, InterruptedException, TimeoutException {
        final Cluster cluster = this.createClusterWithXNumOfConnection(50);


        final Client.ClusteredClient client = cluster.connect();

        final int requests = 50;
        IntStream.range(0, requests).forEach(i -> {
            try {
                client.submitAsync("Thread.sleep(1000);");
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        // Wait for the requests to be sent to the server
        Thread.sleep(2000);

        // Close the cluster abruptly while the requests are in flight
        cluster.close();

        assertThat(recordingAppender.logContainsAny("INFO - Closing active channels borrowed from ChannelPool \\[BusyConnectionCount=50\\]"), is(true));
        assertThat(recordingAppender.logContainsAny("INFO - Closed ConnectionPool\\{closing=true, host=Host\\{address=.*, hostUri=.*\\}, BusyConnectionCount=0\\}"), is(true));
    }

    private Cluster createClusterWithXNumOfConnection(int x) {
        return TestClientFactory.build()
                                .maxConnectionPoolSize(x)
                                .maxSimultaneousUsagePerConnection(0)
                                .minSimultaneousUsagePerConnection(0)
                                .maxInProcessPerConnection(0)
                                .minInProcessPerConnection(0)
                                .create();
    }
}
