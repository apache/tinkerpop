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
package org.apache.tinkerpop.gremlin.server.transaction;

import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Maintains state for an active transaction over HTTP.
 * <p>
 * Key design principle: Graph transactions are ThreadLocal-bound, so all operations
 * for a transaction must execute on the same thread. This is achieved via a
 * single-threaded executor. Callers submit {@link FutureTask} instances that contain
 * the complete request lifecycle (graph operation, error handling, response writing),
 * following the same pattern as the non-transactional HTTP path and the legacy
 * {@code SessionOpProcessor}.
 * <p>
 * The single-threaded executor is a {@link SingleThreadTransactionExecutor} (a {@code ThreadPoolExecutor} with one
 * core/max thread) rather than {@link java.util.concurrent.Executors#newSingleThreadExecutor}. It is behaviorally
 * identical for task execution but exposes the {@code beforeExecute}/{@code afterExecute} lifecycle hooks and the task
 * queue, which the idle-timer management relies on to tell "an operation is running" apart from "the worker is idle
 * with an empty queue". The {@code Executors} factory hides those behind a sealed wrapper.
 */
public class UnmanagedTransaction {
    private static final Logger logger = LoggerFactory.getLogger(UnmanagedTransaction.class);

    private final String transactionId;
    private final String traversalSourceName;
    private final TransactionManager manager;
    private final Graph graph;
    private final ScheduledExecutorService scheduledExecutorService;
    private final long idleTimeout;
    private final long perGraphClose;
    private final AtomicReference<ScheduledFuture<?>> idleFuture = new AtomicReference<>();
    /**
     * The operation currently executing on the worker thread (or most recently submitted) paired with its request
     * {@link Context}, held as a single immutable {@link Running} so the lifetime cap reads a consistent pair — it can
     * never flag one operation's {@code Context} while interrupting another's {@link Future}. The future is the exact
     * same object the per-request evaluation timeout cancels in the handler. Set in {@link #submit} and compare-and-
     * cleared in {@link SingleThreadTransactionExecutor#afterExecute} so a fast next operation is not un-tracked by the
     * previous one's completion.
     */
    private final AtomicReference<Running> current = new AtomicReference<>();
    // Controls whether the executor is still accepting tasks.
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    /**
     * Single-threaded executor ensures all operations for this transaction run on
     * the same thread, preserving the ThreadLocal nature of Graph transactions.
     */
    private final SingleThreadTransactionExecutor executor;

    /**
     * Creates a new {@code UnmanagedTransaction} for managing an HTTP transaction.
     *
     * @param transactionId The unique identifier for this transaction
     * @param transactionManager The manager that owns this transaction's lifecycle
     * @param traversalSourceName The traversal source name bound at begin time
     * @param graph The graph instance for this transaction
     * @param scheduledExecutorService Scheduler for timeout management
     * @param idleTransactionTimeout Inactivity timeout in milliseconds before auto-rollback; {@code 0} disables it
     */
    public UnmanagedTransaction(final String transactionId,
                                final TransactionManager transactionManager,
                                final String traversalSourceName,
                                final Graph graph,
                                final ScheduledExecutorService scheduledExecutorService,
                                final long idleTransactionTimeout,
                                final long perGraphClose) {
        logger.debug("New transaction context established for {}", transactionId);
        this.transactionId = transactionId;
        this.traversalSourceName = traversalSourceName;
        this.manager = transactionManager;
        this.graph = graph;
        this.scheduledExecutorService = scheduledExecutorService;
        this.idleTimeout = idleTransactionTimeout;
        this.perGraphClose = perGraphClose;

        // Create single-threaded executor with named thread for debugging. A ThreadPoolExecutor(1,1) is used (rather
        // than Executors.newSingleThreadExecutor) so the before/afterExecute hooks and the task queue are accessible
        // for idle-timer management; see SingleThreadTransactionExecutor.
        final ThreadFactory threadFactory =
            r -> new Thread(r, "tx-" + transactionId.substring(0, Math.min(8, transactionId.length())));
        this.executor = new SingleThreadTransactionExecutor(threadFactory);
    }

    /**
     * Returns the transaction ID.
     */
    public String getTransactionId() {
        return transactionId;
    }

    /**
     * Returns the traversal source name bound at begin time.
     */
    public String getTraversalSourceName() {
        return traversalSourceName;
    }

    /**
     * Closes this transaction and releases its resources. When {@code force} is {@code false},
     * any open graph transaction is rolled back before shutdown. When {@code force} is {@code true},
     * the executor is shut down immediately without attempting a rollback.
     *
     * @param force if {@code true}, skip the rollback attempt and shut down immediately
     */
    public synchronized void close(boolean force) {
        accepting.set(false);

        // if the transaction has already been removed then there's no need to do this process again. it's possible
        // for this to be called at roughly the same time. this prevents close() from being called more than once.
        if (manager.get(transactionId).isEmpty()) return;

        if (!force) {
            // when not "forced", an open transaction should be rolled back
            try {
                executor.submit(() -> {
                    if (graph.tx().isOpen()) {
                        logger.debug("Rolling back open transaction on {}", transactionId);
                        graph.tx().rollback();
                    }
                }).get(perGraphClose, TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                logger.warn(String.format("An error occurred while attempting rollback on %s ", transactionId), ex);
            }
        }

        // ORDERING IS LOAD-BEARING: manager.destroy(transactionId) MUST happen before executor.shutdown().
        //
        // We use a graceful shutdown() rather than shutdownNow(). A sibling request for this transaction may already
        // be queued behind the commit/rollback that triggered this close (single-threaded executor). shutdownNow()
        // would drain and silently discard those queued tasks, leaving their HTTP clients with no response (a hang) -
        // this was observed as an intermittent CI hang. shutdown() instead lets each queued task run to completion
        // (so every request gets a response) while still terminating the worker thread once the queue drains, so the
        // transaction thread is not leaked. shutdown() also avoids the self-interrupt shutdownNow() caused when close()
        // runs on the tx thread itself (commit/rollback), which could corrupt the in-flight response write.
        //
        // Because those queued tasks now actually RUN, correctness depends on them NOT mutating a committed/rolled-back
        // transaction. Removing the transaction from the manager FIRST guarantees that: when a queued sibling task
        // runs, its pre-evaluation guard (transactionManager.get(txId)) finds nothing and fails fast with a 404
        // (transaction not found) before reaching evaluation. If the destroy were moved after shutdown(), a queued
        // task (e.g. an addV submitted after a commit) could execute against the transaction and leak data. Do not
        // reorder these two statements.
        manager.destroy(transactionId);
        executor.shutdown();
        Optional.ofNullable(idleFuture.get()).ifPresent(f -> f.cancel(true));
        logger.debug("Transaction {} closed", transactionId);
    }

    /**
     * Submits a task to be executed within this transaction's thread context.
     * The task should contain the complete request lifecycle: graph operation,
     * error handling, and response writing.
     *
     * @param task The FutureTask to execute on the transaction thread
     * @param context The request context driving this task, recorded so the lifetime cap can flag it before
     *                interrupting; may be {@code null} for internal operations (such as the begin's tx open) where no
     *                user-facing error needs to be tailored
     * @return Future that can be used for timeout cancellation
     * @throws IllegalStateException if the transaction is closed
     */
    public Future<?> submit(final FutureTask<Void> task, final Context context) {
        if (!accepting.get()) throw new IllegalStateException("Transaction " + transactionId + " is closed");

        // Insurance backstop: cancel (do NOT arm) the idle timer on submit. Arming is the executor's job, done in
        // afterExecute once the worker parks with an empty queue. beforeExecute will also cancel when the task starts;
        // cancelling here too closes the small window between accepting a task and the worker picking it up.
        cancelIdleTimer();

        // Track the running operation BEFORE dispatching it, so the lifetime cap can never miss a worker that starts
        // running between dispatch and tracking. The submitted FutureTask is itself the Future we track and return:
        // cancel(true) on it interrupts the real work (the same future the handler's evaluation timeout cancels), and
        // afterExecute receives this same object to compare-and-clear. Pairing the future with its Context in one
        // immutable Running means the cap always reads a consistent pair (never flags op1 while interrupting op2).
        current.set(new Running(task, context));
        executor.execute(task);
        return task;
    }

    /**
     * Suspends the inactivity timer because an operation is running (or about to run) on the transaction thread.
     * Invoked from {@link SingleThreadTransactionExecutor#beforeExecute} and, as a backstop, from {@link #submit}.
     * <p>
     * A long-running operation must not trip the idle timeout: while an operation is in progress the idle timer is
     * simply not armed (the operation's own duration is bounded by the per-request {@code evaluationTimeout} instead).
     */
    private void cancelIdleTimer() {
        idleFuture.updateAndGet(future -> {
            if (future != null) future.cancel(false);
            return null;
        });
    }

    /**
     * (Re)arms the inactivity timer, but only when the transaction is genuinely idle. Invoked from
     * {@link SingleThreadTransactionExecutor#afterExecute} once an operation has finished and the worker is about to
     * look for more work.
     * <p>
     * "Idle" means: still {@link #accepting} new work (not closing), the executor queue is empty (no sibling request is
     * already waiting — on a single thread there is a brief instant between one task finishing and the next starting),
     * and the idle timeout is enabled ({@code idleTimeout > 0}; {@code 0} disables idle reclamation entirely). When all
     * hold, a fresh {@code close(false)} is scheduled {@code idleTimeout} ms out, replacing any previously scheduled one.
     */
    private void maybeScheduleIdleTimer() {
        if (!accepting.get()) return;            // closing/closed: never re-arm a dying transaction
        if (idleTimeout <= 0) return;            // 0 (or negative) disables idle reclamation
        if (!executor.getQueue().isEmpty()) return; // a sibling task is already queued -> not idle yet

        idleFuture.updateAndGet(future -> {
            if (future != null) future.cancel(false);
            return scheduledExecutorService.schedule(() -> {
                logger.info("Transaction {} timed out after {} ms of inactivity", transactionId, idleTimeout);
                close(false);
            }, idleTimeout, TimeUnit.MILLISECONDS);
        });

        // The accepting check above and the arm below are not atomic: a concurrent close() could have flipped
        // accepting=false and cancelled idleFuture in between, leaving the timer we just armed orphaned (it would fire
        // ~idleTimeout later and call close() on an already-gone transaction). Re-check after arming and cancel if so,
        // so the "never re-arm a dying transaction" invariant actually holds.
        if (!accepting.get()) cancelIdleTimer();
    }

    /**
     * Forcibly tears the transaction down because it has reached its absolute lifetime cap. Invoked by the
     * {@link TransactionManager}'s lifetime timer (the manager owns scheduling and cancelling that timer; this method is
     * the behavior it triggers). Unlike the idle timer, the cap fires regardless of activity, so it may interrupt an
     * operation that is still running.
     * <p>
     * It flags the running operation's {@link Context} <em>before</em> interrupting it so that, as the interrupt unwinds
     * the operation on the worker thread, the error it reports is an accurate transaction-timeout (504) rather than the
     * generic evaluation timeout. It then interrupts only the currently-running operation via
     * {@link Future#cancel(boolean) cancel(true)} (any siblings already queued behind it continue to fail fast with a
     * 404 via the destroy-before-shutdown guard in {@link #close(boolean)}), and finally runs {@code close(false)} to
     * roll back and tear the transaction down. Logged at {@code warn} because this is a forced teardown of active work.
     */
    void onLifetimeCap() {
        // Read the running (future, context) pair once, as a unit, so the Context we flag always belongs to the same
        // operation whose future we interrupt.
        final Running running = current.get();
        if (running != null) {
            if (running.context != null) running.context.setClosedByLifetimeCap(true); // flag BEFORE interrupting
            running.future.cancel(true);                                                // interrupt only the running op
        }

        logger.warn("Transaction {} exceeded its maximum lifetime and is being closed", transactionId);
        close(false);
    }

    /**
     * Compare-and-clears the tracked running operation once it completes. Only clears when the completed future is still
     * the one tracked, so a fast next operation submitted between this one finishing and this clearing is not lost.
     */
    private void clearCurrentExecution(final Future<?> completed) {
        current.updateAndGet(running -> (running != null && running.future == completed) ? null : running);
    }

    /**
     * An in-flight operation paired with the request {@link Context} that drove it, tracked as one immutable unit so the
     * lifetime cap reads a consistent pair. {@code context} may be {@code null} for internal operations (e.g. begin's tx
     * open) that need no tailored client error.
     */
    private static final class Running {
        private final Future<?> future;
        private final Context context;

        private Running(final Future<?> future, final Context context) {
            this.future = future;
            this.context = context;
        }
    }

    /**
     * A single-threaded {@link ThreadPoolExecutor} (one core and max thread) that runs all operations for a single
     * transaction on the same worker thread, preserving the ThreadLocal nature of graph transactions.
     * <p>
     * It is used in place of {@link java.util.concurrent.Executors#newSingleThreadExecutor} solely to expose the
     * {@link #beforeExecute}/{@link #afterExecute} lifecycle hooks (and, via {@link #getQueue()}, the pending-task
     * queue), which the enclosing {@link UnmanagedTransaction} needs to distinguish "an operation is running" from
     * "the worker is idle with nothing queued". Task-execution semantics are otherwise identical to a single-thread
     * executor: one worker, FIFO ordering. Submitted {@link FutureTask}s are returned unwrapped so callers can
     * {@code cancel(true)} the real work (e.g. the per-request evaluation timeout interrupting a running operation).
     */
    private final class SingleThreadTransactionExecutor extends ThreadPoolExecutor {
        private SingleThreadTransactionExecutor(final ThreadFactory threadFactory) {
            super(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory);
        }

        @Override
        protected void beforeExecute(final Thread t, final Runnable r) {
            super.beforeExecute(t, r);
            cancelIdleTimer();
        }

        @Override
        protected void afterExecute(final Runnable r, final Throwable t) {
            super.afterExecute(r, t);
            // For operations submitted via submit(), r is the FutureTask that submit() executed and tracked in
            // `current`, so compare-and-clear by identity un-tracks the operation that just finished without disturbing
            // a faster sibling that may already have replaced it. Other tasks that complete here (e.g. close()'s
            // rollback, which ThreadPoolExecutor also wraps in a Future) simply will not match the tracked future, so
            // the compare-and-clear is a safe no-op for them.
            if (r instanceof Future) clearCurrentExecution((Future<?>) r);
            maybeScheduleIdleTimer();
        }
    }
}
