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

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
 */
public class UnmanagedTransaction {
    private static final Logger logger = LoggerFactory.getLogger(UnmanagedTransaction.class);

    private final String transactionId;
    private final String traversalSourceName;
    private final TransactionManager manager;
    private final Graph graph;
    private final ScheduledExecutorService scheduledExecutorService;
    private final long timeout;
    private final long perGraphClose;
    private final AtomicReference<ScheduledFuture<?>> timeoutFuture = new AtomicReference<>();
    // Controls whether the executor is still accepting tasks.
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    /**
     * Single-threaded executor ensures all operations for this transaction run on
     * the same thread, preserving the ThreadLocal nature of Graph transactions.
     */
    private final ExecutorService executor;

    /**
     * Creates a new {@code UnmanagedTransaction} for managing an HTTP transaction.
     *
     * @param transactionId The unique identifier for this transaction
     * @param transactionManager The manager that owns this transaction's lifecycle
     * @param traversalSourceName The traversal source name bound at begin time
     * @param graph The graph instance for this transaction
     * @param scheduledExecutorService Scheduler for timeout management
     * @param transactionTimeout Timeout in milliseconds before auto-rollback
     */
    public UnmanagedTransaction(final String transactionId,
                                final TransactionManager transactionManager,
                                final String traversalSourceName,
                                final Graph graph,
                                final ScheduledExecutorService scheduledExecutorService,
                                final long transactionTimeout,
                                final long perGraphClose) {
        logger.debug("New transaction context established for {}", transactionId);
        this.transactionId = transactionId;
        this.traversalSourceName = traversalSourceName;
        this.manager = transactionManager;
        this.graph = graph;
        this.scheduledExecutorService = scheduledExecutorService;
        this.timeout = transactionTimeout;
        this.perGraphClose = perGraphClose;

        // Create single-threaded executor with named thread for debugging
        this.executor = Executors.newSingleThreadExecutor(
            r -> new Thread(r, "tx-" + transactionId.substring(0, Math.min(8, transactionId.length()))));
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
     * Resets the timeout for this transaction. Called on each request.
     */
    public void touch() {
        timeoutFuture.updateAndGet(future -> {
            if (future != null) future.cancel(false);
            return scheduledExecutorService.schedule(() -> {
                logger.info("Transaction {} timed out after {} ms of inactivity", transactionId, timeout);
                close(false);
            }, timeout, TimeUnit.MILLISECONDS);
        });
    }

    /**
     * Opens the underlying graph transaction and starts the inactivity timeout.
     * Should be called on the transaction's single-threaded executor to preserve
     * ThreadLocal affinity. On failure the exception is re-thrown and the caller
     * is responsible for cleanup (e.g. via {@link #close(boolean)}).
     */
    public void open() {
        try {
            graph.tx().open();
            touch();
            logger.debug("Transaction {} opened", transactionId);
        } catch (Exception e) {
            logger.warn("Failed to begin transaction {}: {}", transactionId, e.getMessage());
            throw e;
        }
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
        Optional.ofNullable(timeoutFuture.get()).ifPresent(f -> f.cancel(true));
        logger.debug("Transaction {} closed", transactionId);
    }

    /**
     * Submits a task to be executed within this transaction's thread context.
     * The task should contain the complete request lifecycle: graph operation,
     * error handling, and response writing.
     *
     * @param task The FutureTask to execute on the transaction thread
     * @return Future that can be used for timeout cancellation
     * @throws IllegalStateException if the transaction is closed
     */
    public Future<?> submit(final FutureTask<Void> task) {
        if (!accepting.get()) throw new IllegalStateException("Transaction " + transactionId + " is closed");

        touch();
        return executor.submit(task);
    }
}
