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
package org.apache.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A set of methods that allow for control of transactional behavior of a {@link Graph} instance. Providers may
 * consider using {@link AbstractTransaction} as a base implementation that provides default features for most of
 * these methods.
 * <p/>
 * It is expected that this interface be implemented by providers in a {@link ThreadLocal} fashion. In other words
 * transactions are bound to the current thread, which means that any graph operation executed by the thread occurs
 * in the context of that transaction and that there may only be one thread executing in a single transaction.
 * <p/>
 * It is important to realize that this class is not a "transaction object".  It is a class that holds transaction
 * related methods thus hiding them from the {@link Graph} interface.  This object is not meant to be passed around
 * as a transactional context.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author TinkerPop Community (http://tinkerpop.apache.org)
 */
public interface Transaction extends AutoCloseable {

    /**
     * Opens a transaction.
     */
    public void open();

    /**
     * Commits a transaction. This method may optionally throw {@link TransactionException} on error. Providers should
     * consider wrapping their transaction exceptions in this TinkerPop exception as it will lead to better error
     * handling with Gremlin Server and other parts of the stack.
     */
    public void commit();

    /**
     * Rolls back a transaction. This method may optionally throw {@link TransactionException} on error. Providers should
     * consider wrapping their transaction exceptions in this TinkerPop exception as it will lead to better error
     * handling with Gremlin Server and other parts of the stack.
     */
    public void rollback();

    /**
     * Creates a transaction that can be executed across multiple threads. The {@link Graph} returned from this
     * method is not meant to represent some form of child transaction that can be committed from this object.
     * A threaded transaction is a {@link Graph} instance that has a transaction context that enables multiple
     * threads to collaborate on the same transaction.  A standard transactional context tied to a {@link Graph}
     * that supports transactions will typically bind a transaction to a single thread via {@link ThreadLocal}.
     */
    public <G extends Graph> G createThreadedTx();

    /**
     * Determines if a transaction is currently open.
     */
    public boolean isOpen();

    /**
     * An internal function that signals a read or a write has occurred - not meant to be called directly by end users.
     */
    public void readWrite();

    /**
     * Closes the transaction where the default close behavior defined by {{@link #onClose(Consumer)}} will be
     * executed.
     */
    @Override
    public void close();

    /**
     * Describes how a transaction is started when a read or a write occurs.  This value can be set using standard
     * behaviors defined in {@link READ_WRITE_BEHAVIOR} or a mapper {@link Consumer} function.
     */
    public Transaction onReadWrite(final Consumer<Transaction> consumer);

    /**
     * Describes what happens to a transaction on a call to {@link Graph#close()}. This value can be set using
     * standard behavior defined in {@link CLOSE_BEHAVIOR} or a mapper {@link Consumer} function.
     */
    public Transaction onClose(final Consumer<Transaction> consumer);

    /**
     * Adds a listener that is called back with a status when a commit or rollback is successful.  It is expected
     * that listeners be bound to the current thread as is standard for transactions.  Therefore a listener registered
     * in the current thread will not get callback events from a commit or rollback call in a different thread.
     */
    public void addTransactionListener(final Consumer<Status> listener);

    /**
     * Removes a transaction listener.
     */
    public void removeTransactionListener(final Consumer<Status> listener);

    /**
     * Removes all transaction listeners.
     */
    public void clearTransactionListeners();

    /**
     * A status provided to transaction listeners to inform whether a transaction was successfully committed
     * or rolled back.
     */
    public enum Status {
        COMMIT, ROLLBACK
    }

    public static class Exceptions {

        private Exceptions() {
        }

        public static IllegalStateException transactionAlreadyOpen() {
            return new IllegalStateException("Stop the current transaction before opening another");
        }

        public static IllegalStateException transactionMustBeOpenToReadWrite() {
            return new IllegalStateException("Open a transaction before attempting to read/write the transaction");
        }

        public static IllegalStateException openTransactionsOnClose() {
            return new IllegalStateException("Commit or rollback all outstanding transactions before closing the transaction");
        }

        public static UnsupportedOperationException threadedTransactionsNotSupported() {
            return new UnsupportedOperationException("Graph does not support threaded transactions");
        }

        public static IllegalArgumentException onCloseBehaviorCannotBeNull() {
            return new IllegalArgumentException("Transaction behavior for onClose cannot be null");
        }

        public static IllegalArgumentException onReadWriteBehaviorCannotBeNull() {
            return new IllegalArgumentException("Transaction behavior for onReadWrite cannot be null");
        }
    }

    /**
     * Behaviors to supply to the {@link #onClose(Consumer)}. The semantics of these behaviors must be examined in
     * the context of the implementation.  In most cases, these behaviors will be applied as {{@link ThreadLocal}}.
     */
    public enum CLOSE_BEHAVIOR implements Consumer<Transaction> {
        /**
         * Commit the transaction when {@link #close()} is called.
         */
        COMMIT {
            @Override
            public void accept(final Transaction transaction) {
                if (transaction.isOpen()) transaction.commit();
            }
        },

        /**
         * Rollback the transaction when {@link #close()} is called.
         */
        ROLLBACK {
            @Override
            public void accept(final Transaction transaction) {
                if (transaction.isOpen()) transaction.rollback();
            }
        },

        /**
         * Throw an exception if the current transaction is open when {@link #close()} is called.
         */
        MANUAL {
            @Override
            public void accept(final Transaction transaction) {
                if (transaction.isOpen()) throw Exceptions.openTransactionsOnClose();
            }
        }
    }

    /**
     * Behaviors to supply to the {@link #onReadWrite(Consumer)}.
     */
    public enum READ_WRITE_BEHAVIOR implements Consumer<Transaction> {
        /**
         * Transactions are automatically started when a read or a write occurs.
         */
        AUTO {
            @Override
            public void accept(final Transaction transaction) {
                if (!transaction.isOpen()) transaction.open();
            }
        },

        /**
         * Transactions must be explicitly opened for operations to occur on the graph.
         */
        MANUAL {
            @Override
            public void accept(final Transaction transaction) {
                if (!transaction.isOpen()) throw Exceptions.transactionMustBeOpenToReadWrite();
            }
        }
    }
}
