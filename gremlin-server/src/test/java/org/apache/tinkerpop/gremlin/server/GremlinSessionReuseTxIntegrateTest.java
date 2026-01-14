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

import nl.altindag.log.LogCaptor;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

/**
 * Integration tests for gremlin-driver and bytecode sessions where the underlying connection can be re-used for
 * multiple sessions. The server is configured with "closeSessionPostGraphOp" set to True.
 */
public class GremlinSessionReuseTxIntegrateTest extends AbstractSessionTxIntegrateTest {

    public static Pattern CHANNEL_ID_PATTERN = Pattern.compile("SessionedChildClient choosing.*\\{channel=(.*)\\}");

    @Override
    protected Cluster createCluster() {
        return TestClientFactory.build().reuseConnectionsForSessions(true).create();
    }

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        super.overrideSettings(settings);

        // This setting allows connection re-use on the server side.
        settings.closeSessionPostGraphOp = true;

        return settings;
    }

    @Test
    public void shouldCleanupResourcesAfterSuccessfulCommit() throws Exception {
        assumeFalse("Test not supported on deprecated UnifiedChannelizer", isUsingUnifiedChannelizer());

        final LogCaptor logCaptor = LogCaptor.forRoot();
        final Cluster cluster = createCluster();
        try {
            logCaptor.setLogLevelToDebug();
            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

            assertEquals(0, (long) g.V().count().next());

            final GraphTraversalSource gtx = g.tx().begin();
            gtx.addV("person").iterate();
            gtx.tx().commit();

            // Check the both client and server side "sessions" are closed.
            assertClientAndServerSessionResourcesClosed(logCaptor.getLogs());
            assertEquals(1, (long) g.V().count().next());
        } finally {
            cluster.close();
            resetLogCaptor(logCaptor);
        }
    }

    @Test
    public void shouldCleanupResourcesAfterSuccessfulRollback() throws Exception {
        assumeFalse("Test not supported on deprecated UnifiedChannelizer", isUsingUnifiedChannelizer());

        final LogCaptor logCaptor = LogCaptor.forRoot();
        final Cluster cluster = createCluster();
        try {
            logCaptor.setLogLevelToDebug();
            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

            assertEquals(0, (long) g.V().count().next());

            final GraphTraversalSource gtx = g.tx().begin();
            gtx.addV("person").iterate();
            gtx.tx().rollback();

            // Check the both client and server side "sessions" are closed.
            assertClientAndServerSessionResourcesClosed(logCaptor.getLogs());
            assertEquals(0, (long) g.V().count().next());
        } finally {
            cluster.close();
            resetLogCaptor(logCaptor);
        }
    }

    @Test
    public void shouldNotAllowCommittedGtxToBeReused() throws Exception {
        final Cluster cluster = createCluster();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        final GraphTraversalSource gtx = g.tx().begin();
        gtx.addV("person").iterate();
        gtx.tx().commit();

        try {
            gtx.addV("software").iterate();
            gtx.tx().commit();
            fail("gtx that has already been committed shouldn't be reusable.");
        } catch (Exception e) {
            // Consider any exception to be correct behavior.
        }

        cluster.close();
    }

    @Test
    public void shouldAllowMultipleTransactionsOnDifferentConnection() throws Exception {
        final LogCaptor logCaptor = LogCaptor.forRoot();
        final Cluster cluster = createCluster();
        try {
            logCaptor.setLogLevelToDebug();

            final Pattern channelCountPattern = Pattern.compile("Session closed for.*count\\s(.*)");
            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

            final GraphTraversalSource gtx1 = g.tx().begin();
            final GraphTraversalSource gtx2 = g.tx().begin();

            gtx1.addV("person").iterate();
            assertEquals(1, (long) gtx1.V().count().next());

            gtx2.addV("software").iterate();
            assertEquals(1, (long) gtx1.V().count().next());

            gtx1.tx().commit();
            gtx2.tx().commit();

            assertEquals(2, (long) g.V().count().next());

            // Ensure that two different underlying connections were used.
            final Set<String> channelIds = new HashSet<>();
            final List<String> lines = logCaptor.getLogs();
            for (String line : lines) {
                final Matcher idMatcher = CHANNEL_ID_PATTERN.matcher(line);
                final Matcher countMatcher = channelCountPattern.matcher(line);
                if (idMatcher.find()) {
                    channelIds.add(idMatcher.group(1));
                } else if (countMatcher.find()) {
                    // Check that the client properly updates the borrowed count so that it's reset to zero after session
                    // closed, but make the check a bit fuzzy since returning is async and so it may not hit 0 in time
                    assertTrue(Integer.parseInt(countMatcher.group(1)) < 2);
                }
            }

            assertEquals(2, channelIds.size());
        } finally {
            cluster.close();
            resetLogCaptor(logCaptor);
        }
    }

    @Test
    public void shouldAllowMultipleTransactionsOnSameConnection() throws Exception {
        assumeFalse("Test not supported on deprecated UnifiedChannelizer", isUsingUnifiedChannelizer());
        final LogCaptor logCaptor = LogCaptor.forRoot();
        // Cluster setup that has a single connection so simultaneous transactions must share it.
        final Cluster cluster = TestClientFactory.
                build().
                reuseConnectionsForSessions(true).
                minConnectionPoolSize(1).
                maxConnectionPoolSize(1).
                create();
        try {
            logCaptor.setLogLevelToDebug();

            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

            final GraphTraversalSource gtx1 = g.tx().begin();
            final GraphTraversalSource gtx2 = g.tx().begin();

            gtx1.addV("person").iterate();
            gtx2.addV("software").iterate();
            gtx1.addV("place").iterate();

            assertEquals(0, (long) g.V().count().next());
            gtx1.tx().commit();
            assertEquals(2, (long) g.V().count().next());
            assertClientAndServerSessionResourcesClosed(logCaptor.getLogs());

            gtx2.tx().commit();
            assertEquals(3, (long) g.V().count().next());
            assertClientAndServerSessionResourcesClosed(logCaptor.getLogs());
        } finally {
            cluster.close();
            resetLogCaptor(logCaptor);
        }
    }

    @Test
    public void shouldReuseSameConnectionForSubsequentTransactionAfterCommit() throws Exception {
        assumeFalse("Test not supported on deprecated UnifiedChannelizer", isUsingUnifiedChannelizer());
        final LogCaptor logCaptor = LogCaptor.forRoot();
        // Cluster setup with single connection to ensure that transaction state isn't shared even though connection
        // is reused.
        final Cluster cluster = TestClientFactory.build().
                minConnectionPoolSize(1).
                maxConnectionPoolSize(1).
                reuseConnectionsForSessions(true).
                create();
        try {
            logCaptor.setLogLevelToDebug();

            String channelId = "";
            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

            final GraphTraversalSource gtx1 = g.tx().begin();
            gtx1.addV("person1").iterate();
            gtx1.tx().commit();

            List<String> lines = logCaptor.getLogs();
            for (final String line : lines) {
                final Matcher idMatcher = CHANNEL_ID_PATTERN.matcher(line);
                if (idMatcher.find()) {
                    // Save the channelId used for this transaction for comparison with the subsequent one.
                    channelId = idMatcher.group(1);
                }
            }
            assertClientAndServerSessionResourcesClosed(lines);

            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.addV("person2").iterate();
            gtx2.tx().commit();

            lines = logCaptor.getLogs();
            for (final String line : lines) {
                final Matcher idMatcher = CHANNEL_ID_PATTERN.matcher(line);
                if (idMatcher.find()) {
                    // Ensure that same connection was used for both transactions.
                    assertEquals(channelId, idMatcher.group(1));
                }
            }
            assertClientAndServerSessionResourcesClosed(lines);

            assertEquals(2, (long) g.V().count().next());
        } finally {
            cluster.close();
            resetLogCaptor(logCaptor);
        }
    }

    @Test
    public void shouldReuseSameConnectionForSubsequentTransactionAfterRollback() throws Exception {
        assumeFalse("Test not supported on deprecated UnifiedChannelizer", isUsingUnifiedChannelizer());
        final LogCaptor logCaptor = LogCaptor.forRoot();
        final Cluster cluster = TestClientFactory.build().
                minConnectionPoolSize(1).
                maxConnectionPoolSize(1).
                reuseConnectionsForSessions(true).
                create();
        try {
            logCaptor.setLogLevelToDebug();

            String channelId = "";
            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

            final GraphTraversalSource gtx1 = g.tx().begin();
            gtx1.addV("person1").iterate();
            gtx1.tx().rollback();

            List<String> lines = logCaptor.getLogs();
            for (final String line : lines) {
                final Matcher idMatcher = CHANNEL_ID_PATTERN.matcher(line);
                if (idMatcher.find()) {
                    channelId = idMatcher.group(1);
                }
            }
            assertClientAndServerSessionResourcesClosed(lines);

            final GraphTraversalSource gtx2 = g.tx().begin();
            gtx2.addV("person2").iterate();
            gtx2.tx().commit();

            lines = logCaptor.getLogs();
            for (final String line : lines) {
                final Matcher idMatcher = CHANNEL_ID_PATTERN.matcher(line);
                if (idMatcher.find()) {
                    assertEquals(channelId, idMatcher.group(1));
                }
            }
            assertClientAndServerSessionResourcesClosed(lines);

            assertEquals(1, (long) g.V().count().next());
        } finally {
            cluster.close();
            resetLogCaptor(logCaptor);
        }
    }

    // Needed to prevent other long running tests that don't require the LogCaptor from accidentally logging.
    private void resetLogCaptor(final LogCaptor logCaptor) {
        logCaptor.resetLogLevel();
        logCaptor.clearLogs();
        logCaptor.close();
    }

    private void assertClientAndServerSessionResourcesClosed(final List<String> logLines) {
        boolean clientSessionClosed = false;
        boolean serverSessionClosed = false;
        for (final String line : logLines) {
            if (line.matches("Session.*closed$")) {
                serverSessionClosed = true;
            } else if (line.matches("Session closed for Connection.*")) {
                clientSessionClosed = true;
            }
        }
        assertTrue(clientSessionClosed);
        assertTrue(serverSessionClosed);
    }
}
