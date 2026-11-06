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

import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.util.ManualScheduledExecutorService;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link UnmanagedTransaction}, driven by a deterministic {@link ManualScheduledExecutorService} so the
 * inactivity-timeout behaviour can be asserted without real wall-clock waits.
 * <p>
 * These are <em>specification</em> tests for the reworked idle timer (suspend-while-busy): the idle timer is armed only
 * when the transaction goes idle (no operation running, empty queue) and is suspended while an operation runs. The idle
 * timer is (re)armed from the executor's {@code afterExecute} hook, which runs on the transaction worker thread, so
 * timer assertions poll the scheduler with a bounded wait via {@link #awaitPendingTimer(boolean)}.
 */
public class UnmanagedTransactionTest {

    private static final String TX_ID = "test-tx-0001";
    private static final long TIMEOUT_MS = 600000L;
    private static final long PER_GRAPH_CLOSE_MS = 10000L;
    private static final long AWAIT_MS = 5000L;

    private TransactionManager manager;
    private Graph graph;
    private ManualScheduledExecutorService scheduler;
    private UnmanagedTransaction tx;

    @Before
    public void setUp() {
        manager = mock(TransactionManager.class);
        graph = mock(Graph.class);
        final Transaction graphTx = mock(Transaction.class);
        when(graph.tx()).thenReturn(graphTx);
        when(graphTx.isOpen()).thenReturn(false); // rollback path is a no-op during close(false)

        scheduler = new ManualScheduledExecutorService();
        tx = new UnmanagedTransaction(TX_ID, manager, "g", graph, scheduler, TIMEOUT_MS, PER_GRAPH_CLOSE_MS);

        // close() short-circuits unless the manager still knows about the transaction.
        when(manager.get(TX_ID)).thenReturn(Optional.of(tx));
    }

    /**
     * Submits a no-op task and blocks until it has finished running on the worker thread.
     */
    private void runOp() throws Exception {
        tx.submit(new FutureTask<>(() -> null), null).get(AWAIT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Waits (bounded) for the idle timer to reach the expected armed/not-armed state, since it is (re)armed on the
     * worker thread from afterExecute slightly after the submitted task's Future completes. Returns once the condition
     * holds or the wait elapses; the caller asserts on the final state.
     */
    private void awaitPendingTimer(final boolean expectArmed) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(AWAIT_MS);
        while (System.nanoTime() < deadline) {
            if ((scheduler.getPendingTaskCount() == 1) == expectArmed) return;
            Thread.sleep(5);
        }
    }

    @Test
    public void shouldNotScheduleAnyCloseAtConstruction() {
        assertEquals(0, scheduler.getScheduledTaskCount());
        assertEquals(0, scheduler.getPendingTaskCount());
    }

    @Test
    public void shouldArmIdleTimerWhenWorkerGoesIdleAfterAnOperation() throws Exception {
        runOp();

        awaitPendingTimer(true);
        assertEquals("idle timer should be armed once the worker parks with an empty queue",
                1, scheduler.getPendingTaskCount());
        assertEquals(TIMEOUT_MS, scheduler.nextPendingDelayMillis());
    }

    @Test
    public void shouldNotArmIdleTimerWhileAnOperationIsRunning() throws Exception {
        // Hold an operation "running" and assert no idle timer is armed during that window.
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Future<?> running = tx.submit(new FutureTask<>(() -> {
            started.countDown();
            release.await();
            return null;
        }), null);

        assertTrue(started.await(AWAIT_MS, MILLISECONDS));
        // While the op runs, the idle timer must not be armed (a long op must not trip the idle timeout).
        assertEquals(0, scheduler.getPendingTaskCount());

        release.countDown();
        running.get(AWAIT_MS, MILLISECONDS);

        // Once the worker goes idle, the timer arms.
        awaitPendingTimer(true);
        assertEquals(1, scheduler.getPendingTaskCount());
    }

    @Test
    public void shouldNotFireIdleCloseForALongRunningOperation() throws Exception {
        // A single operation that runs longer than the idle timeout must not be reclaimed mid-execution: no timer is
        // armed while it runs, so advancing the clock far past the timeout fires nothing.
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Future<?> running = tx.submit(new FutureTask<>(() -> {
            started.countDown();
            release.await();
            return null;
        }), null);
        assertTrue(started.await(AWAIT_MS, MILLISECONDS));

        scheduler.advanceTimeBy(TIMEOUT_MS * 2, MILLISECONDS);

        verify(manager, never()).destroy(TX_ID);
        release.countDown();
        running.get(AWAIT_MS, MILLISECONDS);
    }

    @Test
    public void shouldCloseTransactionWhenIdleTimeoutFires() throws Exception {
        runOp();
        awaitPendingTimer(true);

        scheduler.advanceTimeBy(TIMEOUT_MS, MILLISECONDS);

        // The scheduled close(false) removes the transaction from the manager.
        verify(manager).destroy(TX_ID);
        assertEquals(0, scheduler.getPendingTaskCount());
    }

    @Test
    public void shouldNotCloseBeforeIdleTimeoutElapses() throws Exception {
        runOp();
        awaitPendingTimer(true);

        scheduler.advanceTimeBy(TIMEOUT_MS - 1, MILLISECONDS);

        verify(manager, never()).destroy(TX_ID);
        assertEquals(1, scheduler.getPendingTaskCount());
    }

    @Test
    public void shouldReArmIdleTimerAfterEachOperation() throws Exception {
        runOp();
        awaitPendingTimer(true);
        assertEquals(1, scheduler.getScheduledTaskCount());

        runOp();
        awaitPendingTimer(true);

        // A second operation cancels the prior idle timer and arms a fresh one.
        assertEquals(2, scheduler.getScheduledTaskCount());
        assertEquals(1, scheduler.getPendingTaskCount());
    }

    @Test
    public void shouldNotArmIdleTimerWhenIdleTimeoutDisabled() throws Exception {
        // idleTransactionTimeout == 0 disables idle reclamation entirely: the timer is never armed.
        final UnmanagedTransaction disabledTx =
                new UnmanagedTransaction(TX_ID, manager, "g", graph, scheduler, 0L, PER_GRAPH_CLOSE_MS);

        disabledTx.submit(new FutureTask<>(() -> null), null).get(AWAIT_MS, TimeUnit.MILLISECONDS);

        awaitPendingTimer(false);
        assertEquals(0, scheduler.getPendingTaskCount());
    }

    @Test
    public void shouldCancelScheduledCloseOnExplicitClose() throws Exception {
        runOp();
        awaitPendingTimer(true);

        tx.close(true);

        verify(manager).destroy(TX_ID);
        // The pending inactivity close must be cancelled so it cannot fire after the transaction is gone.
        assertEquals(0, scheduler.getPendingTaskCount());
    }

    @Test
    public void shouldNotReArmIdleTimerAfterClose() throws Exception {
        runOp();
        awaitPendingTimer(true);

        tx.close(false);

        verify(manager).destroy(TX_ID);
        // Advancing the clock must not resurrect a close on a transaction that is already gone.
        scheduler.advanceTimeBy(TIMEOUT_MS * 2, MILLISECONDS);
        assertEquals(0, scheduler.getPendingTaskCount());
    }

    @Test
    public void shouldRunSubmittedTasksOnASingleNamedTransactionThreadInOrder() throws Exception {
        final List<String> executionOrder = new CopyOnWriteArrayList<>();
        final List<String> threadNames = new CopyOnWriteArrayList<>();

        Future<?> last = null;
        for (int i = 0; i < 5; i++) {
            final int n = i;
            last = tx.submit(new FutureTask<>(() -> {
                threadNames.add(Thread.currentThread().getName());
                executionOrder.add("task-" + n);
                return null;
            }), null);
        }
        last.get(5, TimeUnit.SECONDS); // FIFO single thread: the last task completing means all ran

        assertEquals(List.of("task-0", "task-1", "task-2", "task-3", "task-4"), executionOrder);
        // All ran on one thread, and that thread is the named transaction worker.
        assertEquals(1, threadNames.stream().distinct().count());
        assertTrue("expected tx-* thread but was " + threadNames.get(0),
                threadNames.get(0).startsWith("tx-"));
    }

    @Test
    public void shouldInterruptRunningTaskWhenReturnedFutureIsCancelled() throws Exception {
        // Guards the "do NOT wrap submitted tasks" invariant: cancel(true) on the Future returned by submit() must
        // interrupt the real work, exactly as the per-request evaluation timeout relies on in the handler.
        final CountDownLatch started = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean(false);
        final AtomicReference<Throwable> unexpected = new AtomicReference<>();

        final Future<?> running = tx.submit(new FutureTask<>(() -> {
            started.countDown();
            try {
                Thread.sleep(30000); // block until interrupted by cancel(true)
            } catch (InterruptedException e) {
                interrupted.set(true);
                throw e;
            } catch (Throwable t) {
                unexpected.set(t);
            }
            return null;
        }), null);

        assertTrue("task did not start", started.await(5, TimeUnit.SECONDS));
        running.cancel(true);

        // Give the worker a moment to observe the interrupt and record it.
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!interrupted.get() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue("cancel(true) did not interrupt the running task", interrupted.get());
        assertEquals(null, unexpected.get());
    }

    @Test
    public void shouldCloseTransactionWhenLifetimeCapFiresWhileIdle() {
        // The lifetime timer itself is scheduled/cancelled by the TransactionManager (see TransactionManagerTest); this
        // and the other onLifetimeCap() tests cover what the cap does when it fires, by invoking it directly as the
        // manager's timer would. Here: the cap tears the transaction down even when nothing is running.
        tx.onLifetimeCap();

        verify(manager).destroy(TX_ID);
    }

    @Test
    public void shouldInterruptRunningOperationAndFlagContextWhenLifetimeCapFires() throws Exception {
        final Context ctx = mock(Context.class);
        final CountDownLatch started = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean(false);
        tx.submit(new FutureTask<>(() -> {
            started.countDown();
            try {
                Thread.sleep(30000); // block until the cap interrupts it
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
            return null;
        }), ctx);
        assertTrue("operation did not start", started.await(AWAIT_MS, MILLISECONDS));

        tx.onLifetimeCap();

        // The cap flagged the running request's Context (before interrupting) so the unwinding op reports a 504,
        // interrupted the running operation, and tore the transaction down.
        verify(ctx).setClosedByLifetimeCap(true);
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(AWAIT_MS);
        while (!interrupted.get() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue("lifetime cap did not interrupt the running operation", interrupted.get());
        verify(manager).destroy(TX_ID);
    }

    @Test
    public void shouldFlagAndInterruptTheSameOperationWhenLifetimeCapFires() throws Exception {
        // Guards against a mismatched (future, context) pair: the cap must flag the Context of the very operation whose
        // future it interrupts. op1 completes (clearing its tracking), then op2 runs; when the cap fires it must flag
        // op2's Context and never op1's, and interrupt op2.
        final Context ctx1 = mock(Context.class);
        final Context ctx2 = mock(Context.class);

        tx.submit(new FutureTask<>(() -> null), ctx1).get(AWAIT_MS, MILLISECONDS); // op1 runs to completion

        final CountDownLatch started = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean(false);
        tx.submit(new FutureTask<>(() -> {
            started.countDown();
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
            return null;
        }), ctx2); // op2 is the running op when the cap fires
        assertTrue("op2 did not start", started.await(AWAIT_MS, MILLISECONDS));

        tx.onLifetimeCap();

        verify(ctx2).setClosedByLifetimeCap(true);
        verify(ctx1, never()).setClosedByLifetimeCap(true);
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(AWAIT_MS);
        while (!interrupted.get() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue("the running op (op2) was not interrupted", interrupted.get());
    }

    @Test
    public void shouldClearTrackedExecutionAfterOperationCompletesSoLaterCapFlagsNothing() throws Exception {
        // Compare-and-clear guard: once an operation completes its tracking is cleared, so a cap firing while the
        // transaction is idle finds no running op to flag/interrupt. The tracking is cleared on the worker thread in
        // afterExecute, which races a bare get() on the completed future; the executor is FIFO with one worker, so a
        // second submitted-and-awaited op guarantees the first op's afterExecute (and thus its clear) has already run.
        final Context ctx = mock(Context.class);
        tx.submit(new FutureTask<>(() -> null), ctx);                              // op1 tracks ctx
        tx.submit(new FutureTask<>(() -> null), null).get(AWAIT_MS, MILLISECONDS); // op2 awaited -> op1 cleared

        // The cap still closes the transaction, but op1's Context was cleared (and op2 carried none), so nothing is
        // flagged: a quiet, completed transaction reaching its cap reports no in-flight cap-kill.
        tx.onLifetimeCap();
        verify(ctx, never()).setClosedByLifetimeCap(true);
        verify(manager).destroy(TX_ID);
    }
}
