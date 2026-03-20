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

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RemoteTransaction;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for HTTP remote transactions using the driver API ({@code Cluster.transact()}).
 * 
 * Tests exercise {@link RemoteTransaction} directly via its {@code submit()} methods.
 * 
 * Server-side verification tests (raw HTTP) are in {@link GremlinServerHttpTransactionIntegrateTest}.
 */
public class GremlinDriverTransactionIntegrateTest extends AbstractGremlinServerIntegrationTest {
    private static final String GTX = "gtx";
    private static final int MAX_GET_WAIT = 5000;

    private Cluster cluster;

    @Before
    public void openCluster() {
        cluster = TestClientFactory.open();
    }

    @After
    public void closeCluster() throws Exception {
        if (cluster != null) cluster.close();
    }

    @Override
    public Settings overrideSettings(final Settings settings) {
        settings.channelizer = HttpChannelizer.class.getName();
        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldEnforceMaxConcurrentTransactions":
                settings.maxConcurrentTransactions = 1;
                break;
            case "shouldTimeoutIdleTransaction":
            case "shouldTimeoutIdleTransactionWithNoOperations":
            case "shouldRejectLateCommitAfterTimeout":
                settings.transactionTimeout = 1000;
                break;
            case "shouldTimeoutOnlyIdleTransactionNotActiveOne":
                settings.transactionTimeout = 2000;
                break;
        }
        return settings;
    }

    /**
     * Begin a transaction, add a vertex, verify isolation, commit, verify isOpen transitions, and verify data persists
     * after commit.
     */
    @Test
    public void shouldCommitTransaction() throws Exception {
        final Client client = cluster.connect().alias(GTX);

        final RemoteTransaction tx = cluster.transact(GTX);
        tx.begin();
        // #4: isOpen true after begin
        assertTrue(tx.isOpen());

        tx.submit("g.addV('person').property('name','alice')");

        // #6: uncommitted data not visible outside the transaction
        assertEquals(0L, client.submit("g.V().hasLabel('person').count()").one().getLong());

        tx.commit();
        // #4: isOpen false after commit
        assertFalse(tx.isOpen());
        // #1, #7: committed data visible to non-transactional reads
        assertEquals(1L, client.submit("g.V().hasLabel('person').count()").one().getLong());

        client.close();
    }

    @Test
    public void shouldRollbackTransaction() throws Exception {
        final Client client = cluster.connect().alias(GTX);

        final RemoteTransaction tx = cluster.transact(GTX);
        tx.begin();
        assertTrue(tx.isOpen());

        tx.submit("g.addV('person').property('name','bob')");

        tx.rollback();
        // #5: isOpen false after rollback
        assertFalse(tx.isOpen());
        // #2: data discarded after rollback
        assertEquals(0L, client.submit("g.V().hasLabel('person').count()").one().getLong());

        client.close();
    }

    @Test
    public void shouldSupportIntraTransactionConsistency() throws Exception {
        final Client client = cluster.connect().alias(GTX);

        final RemoteTransaction tx = cluster.transact(GTX);
        tx.begin();
        tx.submit("g.addV('test').property('name','A')");
        // #8: read-your-own-writes — vertex A visible within the transaction
        assertEquals(1L, tx.submit("g.V().hasLabel('test').count()").all().get().get(0).getLong());

        tx.submit("g.addV('test').property('name','B')");
        tx.submit("g.V().has('name','A').addE('knows').to(V().has('name','B'))");

        // verify the full subgraph is visible within the transaction before commit
        assertEquals(2L, tx.submit("g.V().hasLabel('test').count()").all().get().get(0).getLong());
        assertEquals(1L, tx.submit("g.V().outE('knows').count()").all().get().get(0).getLong());

        tx.commit();

        // #3: vertices and edges persist after commit
        assertEquals(2L, client.submit("g.V().hasLabel('test').count()").all().get().get(0).getLong());
        assertEquals(1L, client.submit("g.E().hasLabel('knows').count()").all().get().get(0).getLong());
    }

    @Test
    public void shouldThrowOnSubmitAfterCommit() throws Exception {
        final RemoteTransaction tx = cluster.transact(GTX);
        tx.begin();

        tx.submit("g.addV()");
        tx.commit();

        try {
            tx.submit("g.V().count()");
            fail("Expected exception on submit after commit");
        } catch (IllegalStateException ex) {
            assertThat(ex.getMessage(), containsString("Transaction is not open"));
        }
    }

    @Test
    public void shouldThrowOnSubmitAfterRollback() throws Exception {
        final RemoteTransaction tx = cluster.transact(GTX);
        tx.begin();

        tx.submit("g.addV()");
        tx.rollback();

        try {
            tx.submit("g.V().count()");
            fail("Expected exception on submit after rollback");
        } catch (IllegalStateException ex) {
            assertThat(ex.getMessage(), containsString("Transaction is not open"));
        }
    }

    @Test
    public void shouldThrowOnDoubleBegin() throws Exception {
        final RemoteTransaction tx = cluster.transact(GTX);
        tx.begin();

        try {
            tx.begin();
            fail("Expected IllegalStateException on second begin()");
        } catch (IllegalStateException ex) {
            assertThat(ex.getMessage(), containsString("Transaction already started"));
        }
    }

    @Test
    public void shouldThrowOnCommitWhenNotOpen() throws Exception {
        final RemoteTransaction tx = cluster.transact(GTX);
        assertFalse(tx.isOpen());

        try {
            tx.commit();
            fail("Expected IllegalStateException on commit when not open");
        } catch (IllegalStateException ex) {
            assertThat(ex.getMessage(), containsString("Transaction is not open"));
        }
    }

    @Test
    public void shouldThrowOnRollbackWhenNotOpen() throws Exception {
        final RemoteTransaction tx = cluster.transact(GTX);
        assertFalse(tx.isOpen());

        try {
            tx.rollback();
            fail("Expected IllegalStateException on rollback when not open");
        } catch (IllegalStateException ex) {
            assertThat(ex.getMessage(), containsString("Transaction is not open"));
        }
    }

    @Test
    public void shouldReturnNullTransactionIdBeforeBegin() throws Exception {
        final RemoteTransaction tx = cluster.transact(GTX);

        // before begin, transactionId should be null
        assertNull(tx.getTransactionId());

        tx.begin();
        // after begin, transactionId should be non-null and non-blank
        assertNotNull(tx.getTransactionId());
        assertFalse(tx.getTransactionId().isBlank());
    }

    @Test
    public void shouldCommitOnCloseByDefault() throws Exception {
        final RemoteTransaction tx1 = cluster.transact(GTX);
        tx1.begin();
        tx1.submit("g.addV('person').property('name','close_test')");
        // close() should trigger default COMMIT behavior
        tx1.close();
        assertFalse(tx1.isOpen());

        final RemoteTransaction tx2 = cluster.transact(GTX);
        tx2.begin();
        assertEquals(1L, tx2.submit("g.V().hasLabel('person').count()").one().getLong());
    }

    @Test
    public void shouldRollbackOnCloseWhenConfigured() throws Exception {
        final RemoteTransaction tx1 = cluster.transact(GTX);
        tx1.onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
        tx1.begin();
        tx1.submit("g.addV('person').property('name','rollback_close_test')");
        tx1.close();
        assertFalse(tx1.isOpen());

        final RemoteTransaction tx2 = cluster.transact(GTX);
        tx2.begin();
        assertEquals(0L, tx2.submit("g.V().hasLabel('person').count()").one().getLong());
    }

    @Test
    public void shouldRollbackOpenTransactionsOnClusterClose() throws Exception {
        final RemoteTransaction tx1 = cluster.transact(GTX);
        tx1.begin();
        tx1.submit("g.addV('cluster-close')");
        tx1.submit("g.addV('cluster-close')");

        final RemoteTransaction tx2 = cluster.transact(GTX);
        tx2.begin();
        tx2.submit("g.addV('cluster-close')");

        // close cluster without committing — should rollback open transactions
        cluster.close();
        cluster = null;

        // reconnect and verify data was not persisted
        final Cluster cluster2 = TestClientFactory.open();
        try {
            final RemoteTransaction cluster2tx1 = cluster2.transact(GTX);
            cluster2tx1.begin();
            assertEquals(0L, cluster2tx1.submit("g.V().hasLabel('person').count()").all().get().get(0).getLong());
        } finally {
            cluster2.close();
        }
    }

    @Test
    public void shouldEnforceMaxConcurrentTransactions() throws Exception {
        // first transaction fills the single slot
        final RemoteTransaction tx1 = cluster.transact(GTX);
        tx1.begin();

        // second transaction should fail
        try {
            final RemoteTransaction tx2 = cluster.transact(GTX);
            tx2.begin();
            fail("Expected exception when max concurrent transactions exceeded");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root.getMessage(), containsString("Maximum concurrent transactions exceeded"));
        }
    }

    @Test
    public void shouldTimeoutIdleTransaction() throws Exception {
        final Client client = cluster.connect().alias(GTX);

        final RemoteTransaction tx = cluster.transact(GTX);
        tx.begin();
        tx.submit("g.addV('timeout_test')");

        // wait for the transaction to timeout
        Thread.sleep(2000);

        // the transaction was rolled back server-side; attempting to commit should fail
        try {
            tx.commit();
            fail("Expected exception on commit after server-side timeout");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root.getMessage(), containsString("Transaction not found"));
        }

        // verify data was not persisted
        assertEquals(0L, client.submit("g.V().hasLabel('timeout_test').count()").all().get().get(0).getLong());
    }

    @Test
    public void shouldTimeoutIdleTransactionWithNoOperations() throws Exception {
        final RemoteTransaction tx = cluster.transact(GTX);
        tx.begin();

        // wait for the transaction to timeout
        Thread.sleep(2000);

        // attempting to commit should fail because the server rolled back
        try {
            tx.commit();
            fail("Expected exception on commit after server-side timeout");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root.getMessage(), containsString("Transaction not found"));
        }
    }

    @Test
    public void shouldTimeoutOnlyIdleTransactionNotActiveOne() throws Exception {
        final Client client = cluster.connect().alias(GTX);

        final RemoteTransaction txActive = cluster.transact(GTX);
        txActive.begin();
        txActive.submit("g.addV('active')");

        final RemoteTransaction txIdle = cluster.transact(GTX);
        txIdle.begin();
        txIdle.submit("g.addV('idle')");

        // keep the active transaction alive by sending requests at intervals shorter than timeout
        for (int i = 0; i < 3; i++) {
            Thread.sleep(800);
            txActive.submit("g.V().count()");
        }

        // by now the idle transaction should have timed out (2000ms elapsed)
        // the active transaction should still be alive
        txActive.commit();

        // verify active transaction's data persisted
        assertEquals(1L, client.submit("g.V().hasLabel('active').count()").all().get().get(0).getLong());

        // idle transaction should have been rolled back by timeout
        try {
            txIdle.commit();
            fail("Expected exception on commit of timed-out idle transaction");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root.getMessage(), containsString("Transaction not found"));
        }

        // verify idle transaction's data was not persisted
        assertEquals(0L, client.submit("g.V().hasLabel('idle').count()").all().get().get(0).getLong());
    }

    @Test
    public void shouldRejectLateCommitAfterTimeout() throws Exception {
        final Client client = cluster.connect().alias(GTX);

        final RemoteTransaction tx = cluster.transact(GTX);
        tx.begin();
        tx.submit("g.addV('person').property('name','late_commit')");

        // wait for timeout
        Thread.sleep(2000);

        // attempt commit — should fail because server already rolled back
        try {
            tx.commit();
            fail("Expected exception on late commit after timeout");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root.getMessage(), containsString("Transaction not found"));
        }

        // verify data was not persisted
        assertEquals(0L, client.submit("g.V().hasLabel('person').count()").all().get().get(0).getLong());
    }

    @Test
    public void shouldIsolateConcurrentTransactions() throws Exception {
        final Client client = cluster.connect().alias(GTX);

        final RemoteTransaction tx1 = cluster.transact(GTX);
        tx1.begin();
        final RemoteTransaction tx2 = cluster.transact(GTX);
        tx2.begin();

        tx1.submit("g.addV('tx1')");
        tx2.submit("g.addV('tx2')");

        // tx1 should not see tx2's data and vice versa
        assertEquals(0L, tx1.submit("g.V().hasLabel('tx2').count()").all().get().get(0).getLong());
        assertEquals(0L, tx2.submit("g.V().hasLabel('tx1').count()").all().get().get(0).getLong());

        tx1.commit();
        tx2.commit();

        // both should be visible after commit
        assertEquals(1L, client.submit("g.V().hasLabel('tx1').count()").all().get().get(0).getLong());
        assertEquals(1L, client.submit("g.V().hasLabel('tx2').count()").all().get().get(0).getLong());
    }

    @Test
    public void shouldOpenAndCloseManyTransactionsSequentially() throws Exception {
        final Client client = cluster.connect().alias(GTX);
        final long numberOfTransactions = 50;

        for (int i = 0; i < numberOfTransactions; i++) {
            final RemoteTransaction tx = cluster.transact(GTX);
            tx.begin();
            tx.submit("g.addV('stress')");
            tx.commit();
        }

        final long count = client.submit("g.V().hasLabel('stress').count()").all().get().get(0).getLong();
        assertEquals(numberOfTransactions, count);

        Thread.sleep(100);
        // this should be 0, but to prevent flakiness, make it a reasonable number less than numberOfTransactions
        assertTrue(server.getServerGremlinExecutor().getTransactionManager().getActiveTransactionCount() < 35);
    }

    @Test
    public void shouldIsolateTransactionalAndNonTransactionalRequests() throws Exception {
        final Client client = cluster.connect().alias(GTX);

        final RemoteTransaction tx = cluster.transact(GTX);
        tx.begin();
        tx.submit("g.addV('tx_data')");

        // non-transactional read should not see uncommitted tx data
        assertEquals(0L, client.submit("g.V().hasLabel('tx_data').count()").all().get().get(0).getLong());

        tx.commit();

        // now the data should be visible
        assertEquals(1L, client.submit("g.V().hasLabel('tx_data').count()").all().get().get(0).getLong());
    }

    @Test
    public void shouldRejectBeginOnNonTransactionalGraph() throws Exception {
        final RemoteTransaction tx = cluster.transact("gclassic");
        try {
            tx.begin();
            fail("Expected exception when beginning transaction on non-transactional graph");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root.getMessage(), containsString("Graph does not support transactions"));
        }
    }

    @Test
    public void shouldTargetCorrectGraph() throws Exception {
        final Client client = cluster.connect();

        final RemoteTransaction tx = cluster.transact(GTX);
        tx.begin();

        tx.submit("g.addV('routed')");
        tx.commit();

        // vertex should exist in the transactional graph (gtx)
        assertEquals(1L, client.submit("g.V().hasLabel('routed').count()",
                RequestOptions.build().addG(GTX).create()).all().get().get(0).getLong());

        // vertex should NOT exist in the classic graph (gclassic)
        assertEquals(0L, client.submit("g.V().hasLabel('routed').count()",
                RequestOptions.build().addG("gclassic").create()).all().get().get(0).getLong());
    }

    @Test
    public void shouldAutoCommitNonTransactionalWrite() throws Exception {
        final Client client = cluster.connect();
        client.submit("g.addV('auto')").all().get();
        assertEquals(1L, client.submit("g.V().hasLabel('auto').count()").one().getLong());
    }

    @Test
    public void shouldUseProvidedRequestOptions() throws Exception {
        final Client client = cluster.connect().alias(GTX);
        final RemoteTransaction tx = cluster.transact(GTX);
        tx.begin();
        final RequestOptions ro = RequestOptions.build().
                addParameter("x", "vowels").
                materializeProperties(Tokens.MATERIALIZE_PROPERTIES_TOKENS).
                create();
        final Vertex v = tx.submit("g.addV(x).property('a', 'b')", ro).
                all().
                get(MAX_GET_WAIT, TimeUnit.MILLISECONDS).
                get(0).
                getVertex();
        assertFalse(v.properties().hasNext());
        tx.commit();

        final List<Result> results =
                client.submit("g.V().hasLabel('vowels')").all().get(MAX_GET_WAIT, TimeUnit.MILLISECONDS);
        assertEquals(1L, results.size());
    }

    @Test
    public void shouldUseProvidedParameters() throws Exception {
        final Client client = cluster.connect().alias(GTX);
        final RemoteTransaction tx = cluster.transact(GTX);
        tx.begin();
        final Map params = new HashMap();
        params.put("x", "consonants");
        tx.submit("g.addV(x)", params);
        tx.commit();

        final List<Result> results =
                client.submit("g.V().hasLabel('consonants')").all().get(MAX_GET_WAIT, TimeUnit.MILLISECONDS);
        assertEquals(1L, results.size());
    }

    @Test
    public void shouldCleanUpOnBeginFailure() throws Exception {
        final RemoteTransaction tx = cluster.transact("gclassic");

        try {
            tx.begin();
            fail("Expected exception on begin for non-transactional graph");
        } catch (RuntimeException ex) {
            assertThat(ex.getMessage(), containsString("Failed to begin transaction"));
        }

        // verify cleanup: transaction is closed and has no ID
        assertFalse(tx.isOpen());
        assertNull(tx.getTransactionId());

        // second begin should fail — state moved to CLOSED, not back to NOT_STARTED
        try {
            tx.begin();
            fail("Expected IllegalStateException on begin after failed begin");
        } catch (IllegalStateException ex) {
            assertThat(ex.getMessage(), containsString("Transaction already started"));
        }
    }

    @Test
    public void shouldKeepTransactionOpenAfterTraversalError() throws Exception {
        final Client client = cluster.connect().alias(GTX);

        final RemoteTransaction tx = cluster.transact(GTX);
        tx.begin();
        tx.submit("g.addV('good_vertex')");

        // submit a bad traversal that should fail
        try {
            tx.submit("g.V().fail()");
        } catch (Exception ex) {
            // expected error from bad traversal
        }

        // transaction should still be open — rollback should work
        assertTrue(tx.isOpen());
        tx.rollback();

        assertFalse(tx.isOpen());
        assertEquals(0L, client.submit("g.V().hasLabel('good_vertex').count()").all().get().get(0).getLong());
    }

    @Test
    public void shouldWorkWithDriverRemoteConnection() throws Exception {
        final GraphTraversalSource g = traversal().with(DriverRemoteConnection.using(cluster, GTX));
        final GraphTraversalSource gtx = g.tx().begin();
        gtx.addV("val").iterate();
        gtx.tx().commit();

        assertEquals(1L, g.V().hasLabel("val").count().next().longValue());
    }
}
