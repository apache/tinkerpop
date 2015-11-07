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
package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A base implementation of {@link Transaction} that provides core functionality for transaction listeners using a
 * shared set of transaction listeners.  Therefore, when {@link #commit()} is called from any thread, all listeners
 * get notified.  This implementation would be useful for graph implementations that support threaded transactions,
 * specifically in the {@link Graph} instance returned from {@link Transaction#createThreadedTx()}.
 *
 * @see AbstractThreadLocalTransaction
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractThreadedTransaction extends AbstractTransaction {

    protected final List<Consumer<Status>> transactionListeners = new CopyOnWriteArrayList<>();
    
     public AbstractThreadedTransaction(final Graph g) {
        super(g);
    }

    @Override
    protected void fireOnCommit() {
        transactionListeners.forEach(c -> c.accept(Status.COMMIT));
    }

    @Override
    protected void fireOnRollback() {
        transactionListeners.forEach(c -> c.accept(Status.ROLLBACK));
    }

    @Override
    public void addTransactionListener(final Consumer<Status> listener) {
        transactionListeners.add(listener);
    }

    @Override
    public void removeTransactionListener(final Consumer<Status> listener) {
        transactionListeners.remove(listener);
    }

    @Override
    public void clearTransactionListeners() {
        transactionListeners.clear();
    }

    /**
     * Most implementations should do nothing with this as the tx is already open on creation.
     */
    @Override
    protected void doReadWrite() {
        // do nothing
    }

    /**
     * Clears transaction listeners
     */
    @Override
    protected void doClose() {
        clearTransactionListeners();
    }

    /**
     * The nature of threaded transactions are such that they are always open when created and manual in nature,
     * therefore setting this value is not required.
     *
     * @throws UnsupportedOperationException
     */
    @Override
    public synchronized Transaction onReadWrite(final Consumer<Transaction> consumer) {
        throw new UnsupportedOperationException("Threaded transactions are open when created and in manual mode");
    }

    /**
     * The nature of threaded transactions are such that they are always open when created and manual in nature,
     * therefore setting this value is not required.
     *
     * @throws UnsupportedOperationException
     */
    @Override
    public synchronized Transaction onClose(final Consumer<Transaction> consumer) {
        throw new UnsupportedOperationException("Threaded transactions are open when created and in manual mode");
    }
}
