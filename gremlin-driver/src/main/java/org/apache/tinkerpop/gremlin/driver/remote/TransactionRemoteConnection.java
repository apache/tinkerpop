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

import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnectionException;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.structure.Transaction;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.tinkerpop.gremlin.driver.RequestOptions.getRequestOptions;

/**
 * A {@link RemoteConnection} that routes all submissions through an {@link HttpRemoteTransaction}.
 * <p>
 * This connection adapts the synchronous transaction API to the async {@link RemoteConnection}
 * interface required by {@link org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource}.
 * <p>
 */
class TransactionRemoteConnection implements RemoteConnection {

    private final HttpRemoteTransaction transaction;

    /**
     * Creates a new connection bound to the specified transaction.
     *
     * @param transaction the transaction that owns this connection
     */
    TransactionRemoteConnection(final HttpRemoteTransaction transaction) {
        this.transaction = transaction;
    }

    /**
     * Submits a traversal through the transaction.
     * <p>
     * The submission is synchronous internally but returns a completed future
     * to satisfy the {@link RemoteConnection} interface.
     *
     * @param gremlinLang the traversal to submit
     * @return a completed future with the traversal results
     * @throws RemoteConnectionException if the transaction is not open or submission fails
     */
    @Override
    public <E> CompletableFuture<RemoteTraversal<?, E>> submitAsync(final GremlinLang gremlinLang)
            throws RemoteConnectionException {
        if (!transaction.isOpen()) {
            throw new RemoteConnectionException("Transaction is not open");
        }

        try {
            // Synchronous submission through transaction
            final ResultSet rs = transaction.submit(gremlinLang.getGremlin(), getRequestOptions(gremlinLang));

            final RemoteTraversal<?, E> traversal = new DriverRemoteTraversal<>(rs,
                null,   // client not needed for iteration
                false,  // attachElements
                Optional.empty());

            return CompletableFuture.completedFuture(traversal);
        } catch (Exception e) {
            throw new RemoteConnectionException(e);
        }
    }

    /**
     * Returns the owning transaction.
     *
     * @return the transaction that owns this connection
     */
    @Override
    public Transaction tx() {
        return transaction;
    }

    /**
     * No-op close implementation.
     * <p>
     * The transaction manages its own lifecycle - users should call
     * {@link Transaction#commit()} or {@link Transaction#rollback()} explicitly.
     */
    @Override
    public void close() {
        // Transaction manages its own lifecycle - don't close it here
    }
}
