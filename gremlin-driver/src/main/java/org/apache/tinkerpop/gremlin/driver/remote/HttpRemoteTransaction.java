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
package org.apache.tinkerpop.gremlin.driver.remote;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Host;
import org.apache.tinkerpop.gremlin.driver.RemoteTransaction;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A {@link Transaction} implementation for HTTP-based remote connections.
 * <p>
 * This class provides synchronous, sequential request execution within a transaction context.
 * All requests are pinned to a single host and include the transaction ID (after begin()).
 * <p>
 * Key characteristics:
 * <ul>
 *   <li>Synchronous API only - no submitAsync() methods</li>
 *   <li>Host pinning - all requests go to the same server</li>
 *   <li>Sequential execution - requests block until complete</li>
 * </ul>
 * <p>
 * Usage:
 * <pre>
 * Transaction tx = cluster.transact("g");
 * GraphTraversalSource gtx = tx.begin();
 * gtx.addV("person").property("name", "alice").iterate();
 * tx.commit();
 * </pre>
 *
 * This class is <b>NOT</b> thread-safe.
 */
public class HttpRemoteTransaction implements RemoteTransaction {
    private static final Logger logger = LoggerFactory.getLogger(HttpRemoteTransaction.class);
    private static final long CLOSING_MAX_WAIT_MS = 10000;

    protected Consumer<Transaction> closeConsumer = CLOSE_BEHAVIOR.COMMIT;
    private final Client.PinnedClient pinnedClient;
    private final Cluster cluster;
    private final Host pinnedHost;
    private final String graphAlias;
    private String transactionId;  // null until begin(), set from server response
    private TransactionState state = TransactionState.NOT_STARTED;

    private enum TransactionState {
        NOT_STARTED, OPEN, CLOSED
    }

    /**
     * Creates a new HTTP transaction.
     * <p>
     * The transaction is not started until {@link #begin(Class)} is called.
     * A host is selected at creation time and all requests will be pinned to it.
     *
     * @param pinnedClient the underlying client for connection access
     * @param graphAlias the graph/traversal source alias (e.g., "g")
     * @throws NoHostAvailableException if no hosts are available in the cluster
     */
    public HttpRemoteTransaction(final Client.PinnedClient pinnedClient, final String graphAlias) {
        this.pinnedClient = pinnedClient;
        this.graphAlias = graphAlias;
        this.pinnedHost = pinnedClient.getPinnedHost();
        this.cluster = pinnedClient.getCluster();
    }

    /**
     * Not supported for remote transactions. Use {@link #begin(Class)} instead.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void open() {
        begin();
    }

    /**
     * Starts a transaction and returns a traversal source bound to it.
     * <p>
     * This method sends {@code g.tx().begin()} to the server, which returns
     * the transaction ID. All subsequent requests will include this ID.
     *
     * @param traversalSourceClass the class of the traversal source to create
     * @param <T> the type of the traversal source
     * @return a new traversal source bound to this transaction
     * @throws IllegalStateException if the transaction is already started
     * @throws RuntimeException if the transaction fails to begin
     */
    @Override
    public <T extends TraversalSource> T begin(final Class<T> traversalSourceClass) {
        if (state != TransactionState.NOT_STARTED) {
            throw new IllegalStateException("Transaction already started");
        }
        cluster.trackTransaction(this);

        try {
            // Send begin - no txId attached yet
            final ResultSet rs = submitInternal("g.tx().begin()");
            
            // Server returns the transaction ID
            this.transactionId = extractTransactionId(rs);
            this.state = TransactionState.OPEN;
        } catch (Exception e) {
            cleanUp();
            throw new RuntimeException("Failed to begin transaction: " + e.getMessage(), e);
        }

        // Create RemoteConnection for the traversal source
        final TransactionRemoteConnection txConnection = new TransactionRemoteConnection(this);

        try {
            return traversalSourceClass.getConstructor(RemoteConnection.class).newInstance(txConnection);
        } catch (Exception e) {
            rollback();
            throw new IllegalStateException("Failed to create TraversalSource", e);
        }
    }

    /**
     * Extracts the transaction ID from the begin() response.
     * <p>
     * The server returns the transaction ID as part of the response to g.tx().begin().
     *
     * @param rs the result set from the begin request
     * @return the transaction ID
     */
    private String extractTransactionId(final ResultSet rs) {
        // Wait for all results and extract the transaction ID
        final List<Result> results = rs.all().join();
        if (results.isEmpty()) {
            throw new IllegalStateException("Server did not return transaction ID");
        }
        try {
            final Object id = results.get(0).get(Map.class).get("transactionId");
            if (id == null) throw new IllegalStateException("Server did not return transaction ID");

            final String idStr = id.toString();
            if (idStr.isBlank()) throw new IllegalStateException("Server returned empty transaction ID");

            return idStr;
        } catch (Exception e) {
            throw new IllegalStateException("Server did not return transaction ID");
        }
    }

    /**
     * Commits the transaction.
     * <p>
     * Sends {@code g.tx().commit()} to the server and closes the transaction.
     *
     * @throws IllegalStateException if the transaction is not open
     * @throws TransactionException if the commit fails
     */
    @Override
    public void commit() {
        closeRemoteTransaction("g.tx().commit()");
    }

    /**
     * Rolls back the transaction.
     * <p>
     * Sends {@code g.tx().rollback()} to the server and closes the transaction.
     *
     * @throws IllegalStateException if the transaction is not open
     * @throws TransactionException if the rollback fails
     */
    @Override
    public void rollback() {
        closeRemoteTransaction("g.tx().rollback()");
    }

    private void closeRemoteTransaction(final String closeScript) {
        if (state != TransactionState.OPEN) throw new IllegalStateException("Transaction is not open");

        try {
            submitInternal(closeScript).all().get(CLOSING_MAX_WAIT_MS, TimeUnit.MILLISECONDS);
            cleanUp();
        } catch (Exception e) {
            logger.warn("Failed to {} transaction on {}", closeScript, pinnedHost);
            throw new TransactionException("Failed to " + closeScript, e);
        }
    }

    private void cleanUp() {
        state = TransactionState.CLOSED;
        cluster.untrackTransaction(this);
        pinnedClient.closeAsync();
    }

    /**
     * Returns the server-generated transaction ID, or {@code null} if the transaction
     * has not yet been started via {@link #begin(Class)}.
     *
     * @return the transaction ID, or null if not yet begun
     */
    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public boolean isOpen() {
        return state == TransactionState.OPEN;
    }

    @Override
    public void readWrite() {
        throw new UnsupportedOperationException("Remote transaction behaviors are not configurable - they are always manually controlled");
    }

    @Override
    public void close() {
        closeConsumer.accept(this);
        
        // this is just for safety in case of custom closeConsumer but should normally be handled by commit/rollback
        cleanUp();
    }

    @Override
    public Transaction onReadWrite(final Consumer<Transaction> consumer) {
        throw new UnsupportedOperationException("Remote transaction behaviors are not configurable - they are always manually controlled");
    }

    @Override
    public Transaction onClose(final Consumer<Transaction> consumer) {
        this.closeConsumer = consumer;
        return this;
    }

    @Override
    public void addTransactionListener(final Consumer<Status> listener) {
        throw new UnsupportedOperationException("Remote transactions cannot have listeners attached");
    }

    @Override
    public void removeTransactionListener(final Consumer<Status> listener) {
        throw new UnsupportedOperationException("Remote transactions cannot have listeners attached");
    }

    @Override
    public void clearTransactionListeners() {
        throw new UnsupportedOperationException("Remote transactions cannot have listeners attached");
    }

    @Override
    public ResultSet submit(final String gremlin) {
        return submit(gremlin, RequestOptions.EMPTY);
    }

    @Override
    public ResultSet submit(final String gremlin, final Map<String, Object> parameters) {
        final RequestOptions.Builder builder = RequestOptions.build();
        if (parameters != null && !parameters.isEmpty()) {
            parameters.forEach(builder::addParameter);
        }
        return submit(gremlin, builder.create());
    }

    @Override
    public ResultSet submit(final String gremlin, final RequestOptions options) {
        if (state != TransactionState.OPEN) {
            throw new IllegalStateException("Transaction is not open");
        }
        return submitInternal(gremlin, options);
    }

    private ResultSet submitInternal(final String gremlin) {
        return submitInternal(gremlin, RequestOptions.EMPTY);
    }

    // synchronized here is a bit defensive but ensures that even if a user accidentally uses this in different threads,
    // the server will still receive the requests in the correct order
    private synchronized ResultSet submitInternal(final String gremlin, final RequestOptions options) {
        final RequestOptions.Builder builder = RequestOptions.Builder.from(options);
        if (graphAlias != null) {
            // Don't allow per-request override of "g" as transactions should only target a single Graph instance.
            builder.addG(graphAlias);
        }

        // Attach txId if we have one (not present for begin())
        if (transactionId != null) {
            builder.transactionId(transactionId);
        }

        try {
            return pinnedClient.submit(gremlin, builder.create());
        } catch (Exception e) {
            throw new RuntimeException("Transaction request failed: " + e.getMessage(), e);
        }
    }
}
