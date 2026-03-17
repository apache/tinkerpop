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
package org.apache.tinkerpop.gremlin.server.handler;

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

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Tracks active transactions and returns references of them.
 */
public class TransactionManager {
    private static final Logger logger = LoggerFactory.getLogger(TransactionManager.class);

    private final ConcurrentMap<String, UnmanagedTransaction> transactions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduledExecutorService;
    private final GraphManager graphManager;
    private final long transactionTimeoutMs;
    private final int maxConcurrentTransactions;
    private final long perGraphCloseMs;

    /**
     * Creates a new TransactionManager with the specified configuration.
     *
     * @param scheduledExecutorService Scheduler for timeout management
     * @param graphManager The graph manager for accessing traversal sources
     * @param transactionTimeoutMs Timeout in milliseconds before auto-rollback
     * @param maxConcurrentTransactions Maximum number of concurrent transactions allowed
     */
    public TransactionManager(final ScheduledExecutorService scheduledExecutorService,
                              final GraphManager graphManager,
                              final long transactionTimeoutMs,
                              final int maxConcurrentTransactions,
                              final long perGraphCloseMs) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.graphManager = graphManager;
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.maxConcurrentTransactions = maxConcurrentTransactions;
        this.perGraphCloseMs = perGraphCloseMs;

        MetricManager.INSTANCE.getGauge(transactions::size, name(GremlinServer.class, "transactions"));
        logger.info("TransactionManager initialized with timeout={}ms, maxTransactions={}",
                transactionTimeoutMs, maxConcurrentTransactions);
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

        final UnmanagedTransaction txCtx = createTransactionContext(ts.getGraph());
        logger.debug("Transaction {} created for source {}", txCtx.getTransactionId(), traversalSourceName);
        return txCtx;
    }

    /**
     * Removes a transaction from the active transactions map. Called when a transaction is
     * committed, rolled back, or otherwise closed.
     *
     * @param id The transaction ID to remove
     */
    public void destroy(final String id) {
        transactions.remove(id);
    }

    /**
     * Creates a unique transaction ID, retrying on the unlikely UUID collision. The newly created
     * {@link UnmanagedTransaction} is inserted into the transactions map.
     */
    private UnmanagedTransaction createTransactionContext(final Graph graph) {
        String txId;
        UnmanagedTransaction ctx;

        do {
            txId = UUID.randomUUID().toString();
            ctx = new UnmanagedTransaction(
                    txId,
                    this,
                    graph,
                    scheduledExecutorService,
                    transactionTimeoutMs,
                    perGraphCloseMs
            );
        } while (transactions.putIfAbsent(txId, ctx) != null);

        return ctx;
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
        logger.info("TransactionManager shutdown complete");
    }
}
