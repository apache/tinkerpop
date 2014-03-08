package com.tinkerpop.gremlin.structure;

import java.io.Closeable;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author TinkerPop Community (http://tinkerpop.com)
 */
public interface Transaction extends Closeable {

    public void open();

    public void commit();

    public void rollback();

    public <G extends Graph, R> Workload<G, R> submit(final Function<G, R> work);

    public <G extends Graph> G create();

    public boolean isOpen();

    public void readWrite();

    public void close();

    public Transaction onReadWrite(final Consumer<Transaction> consumer);

    public Transaction onClose(final Consumer<Transaction> consumer);

    public static class Exceptions {
        public static IllegalStateException transactionAlreadyOpen() {
            return new IllegalStateException("Stop the current transaction before opening another");
        }

        public static IllegalStateException transactionMustBeOpenToReadWrite() {
            return new IllegalStateException("Open a transaction before attempting to read/write the transaction");
        }

        public static IllegalStateException openTransactionsOnClose() {
            return new IllegalStateException("Commit or rollback all outstanding transactions before closing the transaction");
        }
    }

    /**
     * Behaviors to supply to the {@link #onClose(java.util.function.Consumer)}.
     */
    public static enum CLOSE_BEHAVIOR implements Consumer<Transaction> {
        /**
         * Any open transaction will commit on close.
         */
        COMMIT {
            @Override
            public void accept(final Transaction transaction) {
                if (transaction.isOpen()) transaction.commit();
            }
        },

        /**
         * Any open transaction will rollback on close.
         */
        ROLLBACK {
            @Override
            public void accept(final Transaction transaction) {
                if (transaction.isOpen()) transaction.rollback();
            }
        },

        /**
         * Open transactions on close will throw an exception
         */
        MANUAL {
            @Override
            public void accept(final Transaction transaction) {
                if(transaction.isOpen()) throw Exceptions.openTransactionsOnClose();
            }
        }
    }

    /**
     * Behaviors to supply to the {@link #onReadWrite(java.util.function.Consumer)}.
     */
    public static enum READ_WRITE_BEHAVIOR implements Consumer<Transaction> {
        AUTO {
            @Override
            public void accept(final Transaction transaction) {
                if (!transaction.isOpen()) transaction.open();
            }
        },

        MANUAL {
            @Override
            public void accept(final Transaction transaction) {
                if (!transaction.isOpen()) throw Exceptions.transactionMustBeOpenToReadWrite();
            }
        }
    }

    /**
     * A {@link Workload} represents a unit of work constructed by the
     * {@link Workload#submit(java.util.function.Function)} method on the {@link Transaction} interface.
     * The unit of work is a {@link java.util.function.Function} typically containing mutations to the {@link Graph}.  The
     * {@link Workload} is responsible for executing the unit of work in the context of a retry strategy, such that a
     * failed unit of work is rolled back and executed again until retries are exhausted or the unit of work succeeds
     * with a commit operation.
     *
     * @param <G> The {@link Graph} instance.
     * @param <R> The type of the result from the unit of work.
     */
    public static class Workload<G extends Graph, R> {
        public static final long DEFAULT_DELAY_MS = 20;
        public static final int DEFAULT_TRIES = 8;

        private final Function<G, R> workToDo;
        private final G g;

        /**
         * Creates a new {@link Workload} that will be tried to be executed within a transaction.
         *
         * @param g    The {@link Graph} instance on which the work will be performed.
         * @param work The work to be executed on the Graph instance which will optionally return a value.
         */
        public Workload(final G g, final Function<G, R> work) {
            this.g = g;
            this.workToDo = work;
        }

        /**
         * Try to execute a {@link Workload} with a custom retry strategy.
         *
         * @param retryStrategy The first argument to this function is the Graph instance and the second is
         *                      the encapsulated work to be performed.  The function should ultimately return the
         *                      result of the encapsulated work function.
         * @return The result of the encapsulated work.
         */
        public R attempt(final BiFunction<G, Function<G, R>, R> retryStrategy) {
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
        private static <G extends Graph, R> BiFunction<G, Function<G, R>, R> retry(final int tries,
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
                        } catch (InterruptedException ie) {
                        }

                    try {
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
