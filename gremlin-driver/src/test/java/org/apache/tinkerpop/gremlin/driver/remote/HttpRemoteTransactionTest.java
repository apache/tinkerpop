/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver.remote;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Host;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link HttpRemoteTransaction} covering its observable lifecycle behavior with a mocked
 * {@link Client.PinnedClient}. These exercise the client-side state machine (begin/commit/rollback/close and their
 * idempotency and error semantics) without a live server. The one behavior that cannot be observed here - that the
 * server actually persists or discards work - is covered by the {@code gremlin-server} integration tests.
 */
public class HttpRemoteTransactionTest {

    private static final String BEGIN = "g.tx().begin()";
    private static final String COMMIT = "g.tx().commit()";
    private static final String ROLLBACK = "g.tx().rollback()";

    private Client.PinnedClient client;
    private Cluster cluster;

    @Before
    public void setUp() {
        client = mock(Client.PinnedClient.class);
        cluster = mock(Cluster.class);
        final Host host = mock(Host.class);

        // Constructor reads the pinned host and cluster from the client; cleanUp() closes the client view.
        when(client.getPinnedHost()).thenReturn(host);
        when(client.getCluster()).thenReturn(cluster);
        when(client.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

        // begin() sends BEGIN and reads the server-assigned id from the first result; every other script (commit,
        // rollback, user submits) resolves to an empty, already-completed result set.
        when(client.submit(anyString(), any(RequestOptions.class))).thenAnswer(inv -> {
            final String gremlin = inv.getArgument(0);
            return BEGIN.equals(gremlin) ? beginResultSet("tx-42") : emptyResultSet();
        });
    }

    /**
     * A result set whose header and body futures are already complete and whose body is empty. Mirrors what the
     * server returns for commit/rollback and lets {@code submitInternal()} (which blocks on {@code
     * headersReceivedAsync()} and {@code all()}) return without a real network round trip.
     */
    private static ResultSet emptyResultSet() {
        final ResultSet rs = mock(ResultSet.class);
        when(rs.headersReceivedAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(rs.all()).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
        return rs;
    }

    /**
     * A begin() response carrying a single result map with the given {@code transactionId}, matching how
     * {@link HttpRemoteTransaction} extracts the id the server assigns.
     */
    @SuppressWarnings("unchecked")
    private static ResultSet beginResultSet(final String transactionId) {
        final Result result = mock(Result.class);
        when(result.get(Map.class)).thenReturn(Collections.singletonMap("transactionId", transactionId));
        final ResultSet rs = mock(ResultSet.class);
        when(rs.headersReceivedAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(rs.all()).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));
        return rs;
    }

    private HttpRemoteTransaction newTransaction() {
        return new HttpRemoteTransaction(client, "g");
    }


    @Test
    public void shouldBeginAndExposeServerAssignedTransactionId() {
        final HttpRemoteTransaction tx = newTransaction();
        final GraphTraversalSource g = tx.begin(GraphTraversalSource.class);

        assertTrue(tx.isOpen());
        assertEquals("tx-42", tx.getTransactionId());
        // begin() must send exactly one begin script and register the transaction with the cluster for tracking.
        verify(client, times(1)).submit(eq(BEGIN), any(RequestOptions.class));
        verify(cluster, times(1)).trackTransaction(tx);
        // a usable, transaction-bound traversal source is returned
        assertNotNull(g);
    }

    @Test
    public void shouldSendCommitScriptOnCommit() {
        final HttpRemoteTransaction tx = newTransaction();
        tx.begin(GraphTraversalSource.class);
        tx.commit();

        verify(client, times(1)).submit(eq(COMMIT), any(RequestOptions.class));
        verify(client, never()).submit(eq(ROLLBACK), any(RequestOptions.class));
        // committing closes the transaction and untracks it
        assertFalse(tx.isOpen());
        verify(cluster, times(1)).untrackTransaction(tx);
    }

    @Test
    public void shouldSendRollbackScriptOnRollback() {
        final HttpRemoteTransaction tx = newTransaction();
        tx.begin(GraphTraversalSource.class);
        tx.rollback();

        verify(client, times(1)).submit(eq(ROLLBACK), any(RequestOptions.class));
        verify(client, never()).submit(eq(COMMIT), any(RequestOptions.class));
        assertFalse(tx.isOpen());
        verify(cluster, times(1)).untrackTransaction(tx);
    }


    @Test
    public void shouldHonorCustomCloseConsumerOverDefaultRollback() {
        final HttpRemoteTransaction tx = newTransaction();
        tx.begin(GraphTraversalSource.class);

        final AtomicBoolean invoked = new AtomicBoolean(false);
        assertEquals(tx, tx.onClose(t -> invoked.set(true)));
        tx.close();

        // a custom onClose consumer replaces the default: it runs, and no rollback is sent to the server
        assertTrue(invoked.get());
        verify(client, never()).submit(eq(ROLLBACK), any(RequestOptions.class));
        verify(client, never()).submit(eq(COMMIT), any(RequestOptions.class));
    }


    @Test
    public void shouldRejectSubmitBeforeBegin() {
        final HttpRemoteTransaction tx = newTransaction();
        assertThrows(IllegalStateException.class, () -> tx.submit("g.V()"));
    }


    @Test
    public void shouldRejectConfigurableTransactionBehaviors() {
        final HttpRemoteTransaction tx = newTransaction();
        // remote transactions are always manually controlled and cannot carry listeners
        assertThrows(UnsupportedOperationException.class, tx::readWrite);
        assertThrows(UnsupportedOperationException.class, () -> tx.onReadWrite(t -> {}));
        assertThrows(UnsupportedOperationException.class, () -> tx.addTransactionListener(s -> {}));
        assertThrows(UnsupportedOperationException.class, () -> tx.removeTransactionListener(s -> {}));
        assertThrows(UnsupportedOperationException.class, tx::clearTransactionListeners);
    }
}
