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

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Tracks active transactions and returns references of them.
 */
public class TransactionManager {
    private static final Logger logger = LoggerFactory.getLogger(TransactionManager.class);

    private final ConcurrentMap<String, UnmanagedTransaction> transactions = new ConcurrentHashMap<>();
    // Absolute-lifetime timers, one per transaction with the cap enabled, keyed by transaction id. The manager owns the
    // lifetime cap because it is scoped to the transaction's existence in the registry (a single fixed schedule), unlike
    // the activity-driven idle timer that the transaction must own to see its executor's running/idle transitions.
    private final ConcurrentMap<String, ScheduledFuture<?>> lifetimeTimers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduledExecutorService;
    private final GraphManager graphManager;
    private final long idleTransactionTimeoutMs;
    private final long maxTransactionLifetimeMs;
    private final int maxConcurrentTransactions;
    private final long perGraphCloseMs;

    /**
     * Creates a new TransactionManager with the specified configuration.
     *
     * @param scheduledExecutorService Scheduler for timeout management
     * @param graphManager The graph manager for accessing traversal sources
     * @param idleTransactionTimeoutMs Inactivity timeout in milliseconds before auto-rollback; {@code 0} disables it
     * @param maxTransactionLifetimeMs Absolute cap in milliseconds on total transaction age; {@code 0} disables it
     * @param maxConcurrentTransactions Maximum number of concurrent transactions allowed
     */
    public TransactionManager(final ScheduledExecutorService scheduledExecutorService,
                              final GraphManager graphManager,
                              final long idleTransactionTimeoutMs,
                              final long maxTransactionLifetimeMs,
                              final int maxConcurrentTransactions,
                              final long perGraphCloseMs) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.graphManager = graphManager;
        this.idleTransactionTimeoutMs = idleTransactionTimeoutMs;
        this.maxTransactionLifetimeMs = maxTransactionLifetimeMs;
        this.maxConcurrentTransactions = maxConcurrentTransactions;
        this.perGraphCloseMs = perGraphCloseMs;

        MetricManager.INSTANCE.getGauge(transactions::size, name(GremlinServer.class, "transactions"));
        logger.info("TransactionManager initialized with idleTransactionTimeout={}ms, maxTransactionLifetime={}ms, maxTransactions={}",
                idleTransactionTimeoutMs, maxTransactionLifetimeMs, maxConcurrentTransactions);
    }

    /**
     * Creates a new {@link UnmanagedTransaction} for the specified traversal source.
     *
     * @param traversalSourceName The traversal source alias (e.g., "g")
     * @return The new {@link UnmanagedTransaction}, ready for task submission
     * @throws IllegalStateException if max transactions exceeded
     * @throws IllegalArgumentException if traversal source not found
     * @throws UnsupportedOperationException if the graph does not support transactions
     */
    public UnmanagedTransaction create(final String traversalSourceName) {
        if (transactions.size() >= maxConcurrentTransactions) {
            throw new IllegalStateException(
                "Maximum concurrent transactions exceeded (" + maxConcurrentTransactions + ")");
        }

        final TraversalSource ts = graphManager.getTraversalSource(traversalSourceName);
        if (ts == null) {
            throw new IllegalArgumentException("Traversal source not found: " + traversalSourceName);
        } else if (!ts.getGraph().features().graph().supportsTransactions()) {
            throw Graph.Exceptions.transactionsNotSupported();
        }

        final UnmanagedTransaction txCtx = createTransactionContext(traversalSourceName, ts.getGraph());
        logger.debug("Transaction {} created for source {}", txCtx.getTransactionId(), traversalSourceName);
        return txCtx;
    }

    /**
     * Removes a transaction from the active transactions map. Package-private so that only
     * {@link UnmanagedTransaction#close(boolean)} can call it — external callers must go
     * through {@code close()} to ensure the executor is shut down.
     */
    void destroy(final String id) {
        transactions.remove(id);
        // Cancel this transaction's lifetime cap (if any) so the one-shot cannot fire after the transaction is gone.
        // Covers every close path uniformly (commit, rollback, idle reclaim, and the cap firing itself, where
        // cancelling the already-running one-shot is a harmless no-op).
        final ScheduledFuture<?> lifetimeTimer = lifetimeTimers.remove(id);
        if (lifetimeTimer != null) lifetimeTimer.cancel(false);
    }

    /**
     * Creates a unique transaction ID, retrying on the unlikely UUID collision. The newly created
     * {@link UnmanagedTransaction} is inserted into the transactions map.
     */
    private UnmanagedTransaction createTransactionContext(final String traversalSourceName, final Graph graph) {
        String transactionId;
        UnmanagedTransaction transaction;

        do {
            transactionId = UUID.randomUUID().toString();
            transaction = new UnmanagedTransaction(
                    transactionId,
                    this,
                    traversalSourceName,
                    graph,
                    scheduledExecutorService,
                    idleTransactionTimeoutMs,
                    perGraphCloseMs
            );
        } while (transactions.putIfAbsent(transactionId, transaction) != null);

        // Schedule the absolute lifetime cap only AFTER the transaction is registered above. The cap's teardown
        // (onLifetimeCap -> close(false)) early-returns if the manager does not yet know about the transaction, so
        // scheduling it before registration could let a pathologically small cap fire into nothing and leave an
        // unreclaimable transaction holding a worker thread and slot. Scheduling after registration guarantees the cap
        // can always tear the transaction down; destroy() cancels it on every close path. The clock effectively starts
        // at construction (registration follows within microseconds), so it bounds total transaction age including begin.
        if (maxTransactionLifetimeMs > 0) {
            final UnmanagedTransaction registered = transaction; // effectively-final copy for the scheduled method ref
            lifetimeTimers.put(transactionId, scheduledExecutorService.schedule(
                    registered::onLifetimeCap, maxTransactionLifetimeMs, TimeUnit.MILLISECONDS));
        }

        return transaction;
    }

    /**
     * Gets an existing {@link UnmanagedTransaction} by ID.
     *
     * @param transactionId The transaction ID to look up
     * @return Optional containing the {@link UnmanagedTransaction} if found, empty otherwise
     */
    public Optional<UnmanagedTransaction> get(final String transactionId) {
        if (null == transactionId) return Optional.empty(); // Prevent NPE from calling get(null) on ConcurrentHashMap
        return Optional.ofNullable(transactions.get(transactionId));
    }

    /**
     * Returns the number of currently active transactions.
     *
     * @return the count of active transactions
     */
    public int getActiveTransactionCount() {
        return transactions.size();
    }

    /**
     * Shuts down the transaction manager, rolling back all active transactions.
     * <p>
     * This method should be called during server shutdown to ensure all transactions
     * are properly cleaned up. It blocks until all rollbacks complete.
     */
    public void shutdown() {
        final int activeCount = transactions.size();
        logger.info("Shutting down TransactionManager with {} active transactions", activeCount);

        if (activeCount == 0) return;

        // Roll back all active transactions
        transactions.values().forEach(transaction -> {
            try {
                transaction.close(false);
            } catch (Exception e) {
                logger.warn("Error rolling back transaction {} during shutdown: {}",
                    transaction.getTransactionId(), e.getMessage());
            }
        });

        transactions.clear();
        // Each close(false) above already cancelled its lifetime timer via destroy(); cancel any stragglers (e.g. a
        // transaction whose close threw) so no scheduled cap outlives the manager.
        lifetimeTimers.values().forEach(timer -> timer.cancel(false));
        lifetimeTimers.clear();
        logger.info("TransactionManager shutdown complete");
    }
}
