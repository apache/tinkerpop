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
package org.apache.tinkerpop.gremlin.server.transaction;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.util.ManualScheduledExecutorService;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TransactionManager}'s ownership of the absolute lifetime cap: the manager schedules the cap
 * timer after a transaction is registered and cancels it when the transaction is destroyed. The cap's <em>behavior</em>
 * when it fires (interrupt + flag + close) is covered in {@code UnmanagedTransactionTest}; here a deterministic
 * {@link ManualScheduledExecutorService} drives the scheduling/cancellation without wall-clock waits.
 */
public class TransactionManagerTest {

    private static final String SOURCE = "g";
    private static final long IDLE_DISABLED = 0L;
    private static final long PER_GRAPH_CLOSE_MS = 10000L;
    private static final long CAP_MS = 5000L;
    private static final int MAX_CONCURRENT = 1000;

    private ManualScheduledExecutorService scheduler;
    private GraphManager graphManager;

    @Before
    public void setUp() {
        scheduler = new ManualScheduledExecutorService();

        // A traversal source whose graph supports transactions, so create() proceeds to build a transaction.
        final Graph graph = mock(Graph.class, RETURNS_DEEP_STUBS);
        when(graph.features().graph().supportsTransactions()).thenReturn(true);
        final Transaction graphTx = mock(Transaction.class);
        when(graph.tx()).thenReturn(graphTx);
        when(graphTx.isOpen()).thenReturn(false); // rollback during close(false) is a no-op

        final TraversalSource ts = mock(TraversalSource.class);
        when(ts.getGraph()).thenReturn(graph);

        graphManager = mock(GraphManager.class);
        when(graphManager.getTraversalSource(SOURCE)).thenReturn(ts);
    }

    private TransactionManager newManager(final long maxLifetimeMs) {
        return new TransactionManager(scheduler, graphManager, IDLE_DISABLED, maxLifetimeMs, MAX_CONCURRENT, PER_GRAPH_CLOSE_MS);
    }

    @Test
    public void shouldNotScheduleLifetimeCapWhenDisabled() {
        final TransactionManager manager = newManager(0L); // cap disabled
        manager.create(SOURCE);

        // No lifetime timer is scheduled when the cap is disabled (idle is also disabled here, so nothing is scheduled).
        assertEquals(0, scheduler.getScheduledTaskCount());
        assertEquals(0, scheduler.getPendingTaskCount());
    }

    @Test
    public void shouldScheduleLifetimeCapAfterRegistrationWhenEnabled() {
        final TransactionManager manager = newManager(CAP_MS);
        manager.create(SOURCE);

        // The cap is scheduled exactly once, for the configured delay, as soon as the transaction is created.
        assertEquals(1, scheduler.getScheduledTaskCount());
        assertEquals(1, scheduler.getPendingTaskCount());
        assertEquals(CAP_MS, scheduler.nextPendingDelayMillis());
    }

    @Test
    public void shouldReclaimTransactionWhenLifetimeCapFires() {
        final TransactionManager manager = newManager(CAP_MS);
        final UnmanagedTransaction tx = manager.create(SOURCE);
        assertEquals(1, manager.getActiveTransactionCount());

        scheduler.advanceTimeBy(CAP_MS, TimeUnit.MILLISECONDS);

        // The cap fired and tore the transaction down: it is no longer tracked and its timer is gone.
        assertEquals(0, manager.getActiveTransactionCount());
        assertEquals(0, scheduler.getPendingTaskCount());
        assertFalse(manager.get(tx.getTransactionId()).isPresent());
    }

    @Test
    public void shouldCancelLifetimeCapWhenTransactionClosed() {
        final TransactionManager manager = newManager(CAP_MS);
        final UnmanagedTransaction tx = manager.create(SOURCE);
        assertEquals(1, scheduler.getPendingTaskCount());

        tx.close(true); // explicit close -> destroy() must cancel the pending cap

        assertEquals(0, scheduler.getPendingTaskCount());
        // Advancing past the cap must not resurrect a close on a transaction that is already gone.
        scheduler.advanceTimeBy(CAP_MS * 2, TimeUnit.MILLISECONDS);
        assertEquals(0, manager.getActiveTransactionCount());
    }

    @Test
    public void shouldScheduleAnIndependentLifetimeCapPerTransaction() {
        final TransactionManager manager = newManager(CAP_MS);
        manager.create(SOURCE);
        manager.create(SOURCE);

        // Each transaction gets its own one-shot cap timer.
        assertEquals(2, scheduler.getScheduledTaskCount());
        assertEquals(2, scheduler.getPendingTaskCount());
        assertEquals(2, manager.getActiveTransactionCount());
    }
}
