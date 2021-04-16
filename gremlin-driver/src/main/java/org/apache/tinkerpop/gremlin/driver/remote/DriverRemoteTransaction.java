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

import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnectionException;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.GraphOp;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.Transaction;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.apache.tinkerpop.gremlin.process.traversal.GraphOp.TX_COMMIT;
import static org.apache.tinkerpop.gremlin.process.traversal.GraphOp.TX_ROLLBACK;

/**
 * A remote {@link Transaction} implementation that is implemented with the Java driver. It is also a proxy for a
 * {@link RemoteConnection} that is bound to a session.
 * <p/>
 * For users, starting a transaction with {@link #begin()} will produce a {@link TraversalSource} that can be used
 * across multiple threads sending the bytecode based requests to a remote session. It is worth noting that the session
 * will process these requests in a serial fashion and not in parallel. Calling {@link #commit()} or
 * {@link #rollback()} will also close the session and no additional traversal can be executed on the
 * {@link TraversalSource}. A fresh call to {@link #begin()} will be required to open a fresh session to work with.
 * The default behavior of {@link #close()} is to commit the transaction.
 */
public class DriverRemoteTransaction implements Transaction, RemoteConnection {

    private final DriverRemoteConnection sessionBasedConnection;

    protected Consumer<Transaction> closeConsumer = CLOSE_BEHAVIOR.COMMIT;

    public DriverRemoteTransaction(final DriverRemoteConnection sessionBasedConnection) {
        this.sessionBasedConnection = sessionBasedConnection;
    }

    @Override
    public <T extends TraversalSource> T begin(final Class<T> traversalSourceClass) {
        if (!isOpen())
            throw new IllegalStateException("Transaction cannot begin as the session is already closed - create a new Transaction");

        try {
            return traversalSourceClass.getConstructor(RemoteConnection.class).newInstance(this);
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * By virtue of creating a {@code DriverRemoteTransaction}, the transaction is considered open. There is no need
     * to call this method. Calling it when the transaction is closed will result in exception.
     */
    @Override
    public void open() {
        // no need to issue a command to open the transaction, the server is already in such a state if the
        if (!isOpen())
            throw new IllegalStateException("Transaction cannot be opened as the session is already closed - create a new Transaction");
    }

    @Override
    public void commit() {
        closeRemoteTransaction(TX_COMMIT, "Transaction commit for %s failed");
    }

    @Override
    public void rollback() {
        closeRemoteTransaction(TX_ROLLBACK, "Transaction rollback for %s failed");
    }

    private void closeRemoteTransaction(final GraphOp closeTxWith, final String failureMsg) {
        try {
            // kinda weird but we hasNext() the graph command here to ensure that it runs to completion or
            // else you don't guarantee that we have the returned NO_CONTENT message in hand before proceeding
            // which could mean the transaction is still in the process of committing. not sure why iterate()
            // doesn't quite work in this context.
            this.sessionBasedConnection.submitAsync(closeTxWith.getBytecode()).join().hasNext();
            this.sessionBasedConnection.close();
        } catch (Exception ex) {
            throw new RuntimeException(String.format(failureMsg, sessionBasedConnection.getSessionId()), ex);
        }
    }

    @Override
    public boolean isOpen() {
        // for tx purposes closing is a good enough check
        return !sessionBasedConnection.client.isClosing();
    }

    /**
     * The default close behavior for this {@link Transaction} implementation is to {@link #commit()}.
     */
    @Override
    public void close() {
        this.closeConsumer.accept(this);
    }

    /**
     * This {@link Transaction} implementation is not auto-managed and therefore this method is not supported.
     */
    @Override
    public void readWrite() {
        throw new UnsupportedOperationException("Remote transaction behaviors are not auto-managed - they are always manually controlled");
    }

    /**
     * This {@link Transaction} implementation is not auto-managed and therefore this method is not supported.
     */
    @Override
    public Transaction onReadWrite(final Consumer<Transaction> consumer) {
        throw new UnsupportedOperationException("Remote transaction behaviors are not configurable - they are always manually controlled");
    }

    @Override
    public Transaction onClose(final Consumer<Transaction> consumer) {
        this.closeConsumer = consumer;
        return this;
    }

    /**
     * There is no support for remote transaction listeners.
     */
    @Override
    public void addTransactionListener(final Consumer<Status> listener) {
        throw new UnsupportedOperationException("Remote transactions cannot have listeners attached");
    }

    /**
     * There is no support for remote transaction listeners.
     */
    @Override
    public void removeTransactionListener(final Consumer<Status> listener) {
        throw new UnsupportedOperationException("Remote transactions cannot have listeners attached");
    }

    /**
     * There is no support for remote transaction listeners.
     */
    @Override
    public void clearTransactionListeners() {
        throw new UnsupportedOperationException("Remote transactions cannot have listeners attached");
    }

    /**
     * It is not possible to have child transactions, therefore this method always returns {@link Transaction#NO_OP}.
     */
    @Override
    public Transaction tx() {
        return Transaction.NO_OP;
    }

    @Override
    public <E> CompletableFuture<RemoteTraversal<?, E>> submitAsync(final Bytecode bytecode) throws RemoteConnectionException {
        return sessionBasedConnection.submitAsync(bytecode);
    }
}
