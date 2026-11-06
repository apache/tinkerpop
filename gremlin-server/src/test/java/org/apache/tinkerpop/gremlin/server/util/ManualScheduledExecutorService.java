/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A deterministic, single-threaded test double for {@link ScheduledExecutorService} backed by a virtual clock.
 * <p>
 * It exists so timer-driven behavior (such as the {@code gremlin-server} transaction idle / lifetime timeouts) can be
 * exercised without {@link Thread#sleep(long)} and without real wall-clock waits, which are slow and flaky. Time only
 * advances when the test calls {@link #advanceTimeBy(long, TimeUnit)} (or {@link #runDueTasks()}), at which point any
 * scheduled task whose trigger time has been reached is run synchronously on the calling thread, in trigger-time order.
 * <p>
 * Only the one-shot {@link #schedule(Runnable, long, TimeUnit)} overload is implemented, because that is all the
 * transaction code under test uses. Every other {@link ScheduledExecutorService} method throws
 * {@link UnsupportedOperationException} with an explanatory message so that an unsupported use is loud rather than
 * silently wrong.
 * <p>
 * It is thread-safe: the transaction idle timer is armed/cancelled on the transaction's worker thread (via the
 * executor's before/afterExecute hooks) while a test thread advances the clock and reads counts. All shared state is
 * guarded by {@code lock}. Fired task commands are run <em>outside</em> the lock — a fired idle close calls
 * {@code close(false)}, which submits a rollback to the transaction executor and blocks on it; running it under the
 * lock could deadlock against the worker thread re-entering {@link #schedule}.
 */
public class ManualScheduledExecutorService implements ScheduledExecutorService {

    private final Object lock = new Object();
    private final List<ScheduledTask> tasks = new ArrayList<>();
    private long nowMillis = 0L;
    private int scheduledCount = 0;

    /**
     * Schedules a one-shot task to run when the virtual clock advances by at least {@code delay}. Returns a
     * {@link ScheduledFuture} whose {@link Future#cancel(boolean)} prevents the task from running on a later advance.
     */
    @Override
    public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit) {
        synchronized (lock) {
            final ScheduledTask task = new ScheduledTask(command, nowMillis + unit.toMillis(delay));
            tasks.add(task);
            scheduledCount++;
            return task;
        }
    }

    /**
     * Advances the virtual clock by the given amount and runs every task that is now due (trigger time {@code <=} the
     * new current time) and not cancelled, in ascending trigger-time order.
     */
    public void advanceTimeBy(final long amount, final TimeUnit unit) {
        synchronized (lock) {
            nowMillis += unit.toMillis(amount);
        }
        runDueTasks();
    }

    /**
     * Runs every currently-due, non-cancelled task without advancing the clock. Useful for firing a zero-delay task.
     * Each due task is selected under the lock but executed outside it (see class Javadoc).
     */
    public void runDueTasks() {
        while (true) {
            final ScheduledTask next;
            synchronized (lock) {
                ScheduledTask soonest = null;
                // Loop because a fired task may schedule another task that is itself immediately due.
                for (final ScheduledTask t : tasks) {
                    if (!t.cancelled && !t.done && t.triggerAtMillis <= nowMillis) {
                        if (soonest == null || t.triggerAtMillis < soonest.triggerAtMillis) soonest = t;
                    }
                }
                if (soonest == null) return;
                soonest.done = true; // mark done under lock so it is not re-selected
                next = soonest;
            }
            next.command.run(); // run OUTSIDE the lock
        }
    }

    /**
     * The number of tasks that are still scheduled to run (not cancelled, not yet fired).
     */
    public int getPendingTaskCount() {
        synchronized (lock) {
            int count = 0;
            for (final ScheduledTask t : tasks) {
                if (!t.cancelled && !t.done) count++;
            }
            return count;
        }
    }

    /**
     * The total number of tasks ever scheduled, including ones later cancelled or already fired. Lets a test assert
     * that a reschedule actually issued a fresh {@code schedule(...)} call.
     */
    public int getScheduledTaskCount() {
        synchronized (lock) {
            return scheduledCount;
        }
    }

    /**
     * The remaining delay (ms) until the soonest still-pending task fires, or {@code -1} if none is pending.
     */
    public long nextPendingDelayMillis() {
        synchronized (lock) {
            long soonest = Long.MAX_VALUE;
            for (final ScheduledTask t : tasks) {
                if (!t.cancelled && !t.done) soonest = Math.min(soonest, t.triggerAtMillis - nowMillis);
            }
            return soonest == Long.MAX_VALUE ? -1L : soonest;
        }
    }

    private final class ScheduledTask implements ScheduledFuture<Object> {
        private final Runnable command;
        private final long triggerAtMillis;
        private volatile boolean cancelled = false;
        private volatile boolean done = false;

        private ScheduledTask(final Runnable command, final long triggerAtMillis) {
            this.command = command;
            this.triggerAtMillis = triggerAtMillis;
        }

        @Override
        public long getDelay(final TimeUnit unit) {
            synchronized (lock) {
                return unit.convert(triggerAtMillis - nowMillis, TimeUnit.MILLISECONDS);
            }
        }

        @Override
        public int compareTo(final Delayed o) {
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
        }

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            if (done || cancelled) return false;
            cancelled = true;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return done || cancelled;
        }

        @Override
        public Object get() {
            return null;
        }

        @Override
        public Object get(final long timeout, final TimeUnit unit) {
            return null;
        }
    }

    // ---- Unsupported ScheduledExecutorService surface: fail loudly rather than behave unexpectedly. ----

    private static UnsupportedOperationException unsupported(final String method) {
        return new UnsupportedOperationException(
                ManualScheduledExecutorService.class.getSimpleName() + " does not support " + method
                        + "; only schedule(Runnable, long, TimeUnit) is implemented for tests.");
    }

    @Override
    public <V> ScheduledFuture<V> schedule(final Callable<V> callable, final long delay, final TimeUnit unit) {
        throw unsupported("schedule(Callable, long, TimeUnit)");
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, final long initialDelay, final long period, final TimeUnit unit) {
        throw unsupported("scheduleAtFixedRate");
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, final long initialDelay, final long delay, final TimeUnit unit) {
        throw unsupported("scheduleWithFixedDelay");
    }

    @Override
    public void execute(final Runnable command) {
        throw unsupported("execute");
    }

    @Override
    public void shutdown() {
        throw unsupported("shutdown");
    }

    @Override
    public List<Runnable> shutdownNow() {
        throw unsupported("shutdownNow");
    }

    @Override
    public boolean isShutdown() {
        throw unsupported("isShutdown");
    }

    @Override
    public boolean isTerminated() {
        throw unsupported("isTerminated");
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) {
        throw unsupported("awaitTermination");
    }

    @Override
    public <T> Future<T> submit(final Callable<T> task) {
        throw unsupported("submit(Callable)");
    }

    @Override
    public <T> Future<T> submit(final Runnable task, final T result) {
        throw unsupported("submit(Runnable, T)");
    }

    @Override
    public Future<?> submit(final Runnable task) {
        throw unsupported("submit(Runnable)");
    }

    @Override
    public <T> List<Future<T>> invokeAll(final java.util.Collection<? extends Callable<T>> tasks) {
        throw unsupported("invokeAll");
    }

    @Override
    public <T> List<Future<T>> invokeAll(final java.util.Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit) {
        throw unsupported("invokeAll");
    }

    @Override
    public <T> T invokeAny(final java.util.Collection<? extends Callable<T>> tasks) throws ExecutionException {
        throw unsupported("invokeAny");
    }

    @Override
    public <T> T invokeAny(final java.util.Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit) throws ExecutionException {
        throw unsupported("invokeAny");
    }
}
