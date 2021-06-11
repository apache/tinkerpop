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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
            // see TINKERPOP-2504
            Thread.sleep(3000);

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
}
