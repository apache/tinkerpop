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

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A simple base class for {@link Transaction} that provides some common functionality and default behavior.
 * While vendors can choose to use this class, it is generally better to extend from
 * {@link AbstractThreadedTransaction} or {@link AbstractThreadLocalTransaction} which include default "listener"
 * functionality.  Implementers should note that this class assumes that threaded transactions are not enabled
 * and should explicitly override {@link #createThreadedTx} to implement that functionality if required.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractTransaction implements Transaction {
    private Graph g;

    public AbstractTransaction(final Graph g) {
        this.g = g;
    }

    /**
     * Called within {@link #open} if it is determined that the transaction is not yet open given {@link #isOpen}.
     * Implementers should assume the transaction is not yet started and should thus open one.
     */
    protected abstract void doOpen();

    /**
     * Called with {@link #commit} after the {@link #onReadWrite(Consumer)} has been notified.  Implementers should
     * include their commit logic here.
     */
    protected abstract void doCommit() throws TransactionException;

    /**
     * Called with {@link #rollback} after the {@link #onReadWrite(Consumer)} has been notified.  Implementers should
     * include their rollback logic here.
     */
    protected abstract void doRollback() throws TransactionException;

    /**
     * Called within {@link #commit()} just after the internal call to {@link #doCommit()}. Implementations of this
     * method should raise {@link Status#COMMIT} events to any listeners added via
     * {@link #addTransactionListener(Consumer)}.
     */
    protected abstract void fireOnCommit();

    /**
     * Called within {@link #rollback()} just after the internal call to {@link #doRollback()} ()}. Implementations
     * of this method should raise {@link Status#ROLLBACK} events to any listeners added via
     * {@link #addTransactionListener(Consumer)}.
     */
    protected abstract void fireOnRollback();
    
    /**
     * Called {@link #readWrite}.  
     * Implementers should run their readWrite consumer here.
     */
    protected abstract void doReadWrite();
    
    /**
     * Called {@link #close}.  
     * Implementers should run their readWrite consumer here.
     */
    protected abstract void doClose();

    /**
     * {@inheritDoc}
     */
    @Override
    public void open() {
        if (isOpen())
            throw Transaction.Exceptions.transactionAlreadyOpen();
        else
            doOpen();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit() {
        readWrite();
        try {
            doCommit();
            fireOnCommit();
        } catch (TransactionException te) {
            throw new RuntimeException(te);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollback() {
        readWrite();
        try {
            doRollback();
            fireOnRollback();
        } catch (TransactionException te) {
            throw new RuntimeException(te);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R> Workload<R> submit(final Function<Graph, R> work) {
        return new Workload<>(g, work);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public <G extends Graph> G createThreadedTx() {
        throw Transaction.Exceptions.threadedTransactionsNotSupported();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readWrite() {
        doReadWrite();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        doClose();
    }

    /**
     * An "internal" exception thrown by vendors when calls to {@link AbstractTransaction#doCommit} or
     * {@link AbstractTransaction#doRollback} fail.
     */
    public static class TransactionException extends Exception {
        public TransactionException(final String message) {
            super(message);
        }

        public TransactionException(final Throwable cause) {
            super(cause);
        }

        public TransactionException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
