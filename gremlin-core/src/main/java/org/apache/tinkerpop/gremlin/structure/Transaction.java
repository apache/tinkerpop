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

import java.util.Collections;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A set of methods that allow for control of transactional behavior of a {@link Graph} instance. Vendors may
 * consider using {@link AbstractTransaction} as a base implementation that provides default features for most of
 * these methods.
 * <p/>
 * It is expected that this interface be implemented by vendors in a {@link ThreadLocal} fashion. In other words
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
     * Commits a transaction.
     */
    public void commit();

    /**
     * Rolls back a transaction.
     */
    public void rollback();

    /**
     * Submit a unit of work that represents a transaction returning a {@link Workload} that can be automatically
     * retried in the event of failure.
     */
    public <R> Workload<R> submit(final Function<Graph, R> work);

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

    /**
     * A {@link Workload} represents a unit of work constructed by the {@link Workload#submit(Function)} method on
     * the {@link Transaction} interface. The unit of work is a {@link Function} typically containing mutations to
     * the {@link Graph}.  The {@link Workload} is responsible for executing the unit of work in the context of a
     * retry strategy, such that a failed unit of work is rolled back and executed again until retries are exhausted
     * or the unit of work succeeds with a commit operation.
     *
     * @param <R> The type of the result from the unit of work.
     */
    public static class Workload<R> {
        public static final long DEFAULT_DELAY_MS = 20;
        public static final int DEFAULT_TRIES = 8;

        private final Function<Graph, R> workToDo;
        private final Graph g;

        /**
         * Creates a new {@link Workload} that will be tried to be executed within a transaction.
         *
         * @param g    The {@link Graph} instance on which the work will be performed.
         * @param work The work to be executed on the Graph instance which will optionally return a value.
         */
        public Workload(final Graph g, final Function<Graph, R> work) {
            this.g = g;
            this.workToDo = work;
        }

        /**
         * Try to execute a {@link Workload} with a mapper retry strategy.
         *
         * @param retryStrategy The first argument to this function is the Graph instance and the second is
         *                      the encapsulated work to be performed.  The function should ultimately return the
         *                      result of the encapsulated work function.
         * @return The result of the encapsulated work.
         */
        public R attempt(final BiFunction<Graph, Function<Graph, R>, R> retryStrategy) {
            return retryStrategy.apply(g, workToDo);
        }

        /**
         * Executes the {@link Workload} committing if possible and rolling back on failure.  On failure, an exception
         * is reported.
         */
        public R oneAndDone() {
            return attempt((g, w) -> {
                try {
                    R result = w.apply(g);
                    g.tx().commit();

                    return result;
                } catch (Throwable t) {
                    g.tx().rollback();
                    throw new RuntimeException(t);
                }
            });
        }

        /**
         * Executes the {@link Workload} committing if possible and rolling back on failure.  On failure no exception
         * is reported.
         */
        public R fireAndForget() {
            return attempt((g, w) -> {
                R result = null;
                try {
                    result = w.apply(g);
                    g.tx().commit();
                } catch (Throwable t) {
                    g.tx().rollback();
                }

                return result;
            });
        }

        /**
         * Executes the {@link Workload} with the default number of retries and with the default number of
         * milliseconds delay between each try.
         */
        public R retry() {
            return retry(DEFAULT_TRIES);
        }

        /**
         * Executes the {@link Workload} with a number of retries and with the default number of milliseconds delay
         * between each try.
         */
        public R retry(final int tries) {
            return retry(tries, DEFAULT_DELAY_MS);
        }

        /**
         * Executes the {@link Workload} with a number of retries and with a number of milliseconds delay between
         * each try.
         */
        public R retry(final int tries, final long delay) {
            return retry(tries, delay, Collections.emptySet());
        }

        /**
         * Executes the {@link Workload} with a number of retries and with a number of milliseconds delay between each
         * try and will only retry on the set of supplied exceptions.  Exceptions outside of that set will generate a
         * RuntimeException and immediately fail.
         */
        public R retry(final int tries, final long delay, final Set<Class> exceptionsToRetryOn) {
            return attempt(retry(tries, exceptionsToRetryOn, i -> delay));
        }

        /**
         * Executes the {@link Workload} with the default number of retries and with a exponentially increasing
         * number of milliseconds between each retry using the default retry delay.
         */
        public R exponentialBackoff() {
            return exponentialBackoff(DEFAULT_TRIES);
        }

        /**
         * Executes the {@link Workload} with a number of retries and with a exponentially increasing number of
         * milliseconds between each retry using the default retry delay.
         */
        public R exponentialBackoff(final int tries) {
            return exponentialBackoff(tries, DEFAULT_DELAY_MS);
        }

        /**
         * Executes the {@link Workload} with a number of retries and with a exponentially increasing number of
         * milliseconds between each retry.
         */
        public R exponentialBackoff(final int tries, final long initialDelay) {
            return exponentialBackoff(tries, initialDelay, Collections.emptySet());
        }

        /**
         * Executes the {@link Workload} with a number of retries and with a exponentially increasing number of
         * milliseconds between each retry.  It will only retry on the set of supplied exceptions.  Exceptions outside
         * of that set will generate a {@link RuntimeException} and immediately fail.
         */
        public R exponentialBackoff(final int tries, final long initialDelay, final Set<Class> exceptionsToRetryOn) {
            return attempt(retry(tries, exceptionsToRetryOn, retryCount -> (long) (initialDelay * Math.pow(2, retryCount))));
        }

        /**
         * Creates a generic retry function to be passed to the {@link Workload#attempt(java.util.function.BiFunction)}
         * method.
         */
        private static <R> BiFunction<Graph, Function<Graph, R>, R> retry(final int tries,
                                                                          final Set<Class> exceptionsToRetryOn,
                                                                          final Function<Integer, Long> delay) {
            return (g, w) -> {
                R returnValue;

                // this is the default exception...it may get reassgined during retries
                Exception previousException = new RuntimeException("Exception initialized when trying commit");

                // try to commit a few times
                for (int ix = 0; ix < tries; ix++) {

                    // increase time after each failed attempt though there is no delay on the first try
                    if (ix > 0)
                        try {
                            Thread.sleep(delay.apply(ix));
                        } catch (InterruptedException ignored) {
                        }

                    try {
                        // ensure that a transaction is open for this try. even if there was an open transaction
                        // from the first try it would be rolled back on failure and unless automatic transactions
                        // are used, the transaction would be closed on the next retry.
                        if (!g.tx().isOpen()) g.tx().open();

                        returnValue = w.apply(g);
                        g.tx().commit();

                        // need to exit the function here so that retries don't happen
                        return returnValue;
                    } catch (Exception ex) {
                        g.tx().rollback();

                        // retry if this is an allowed exception otherwise, just throw and go
                        boolean retry = false;
                        if (exceptionsToRetryOn.size() == 0)
                            retry = true;
                        else {
                            for (Class exceptionToRetryOn : exceptionsToRetryOn) {
                                if (ex.getClass().equals(exceptionToRetryOn)) {
                                    retry = true;
                                    break;
                                }
                            }
                        }

                        if (!retry) {
                            throw new RuntimeException(ex);
                        }

                        previousException = ex;
                    }
                }

                // the exception just won't go away after all the retries
                throw new RuntimeException(previousException);
            };
        }
    }

}
