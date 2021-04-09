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

import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A special {@code ThreadPoolExecutor} which will construct {@link SessionFutureTask} instances and inject the
 * current running thread into a {@link Session} instance if one is present.
 */
public class SessionExecutor extends ThreadPoolExecutor {

    public SessionExecutor(final int nThreads, final ThreadFactory threadFactory) {
        super(nThreads, nThreads,0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory);
    }

    /**
     * Starts a {@link Session} running in a particular thread selected from the pool. The {@code Future} for
     * completion of the running session is assigned to the session itself
     */
    public Future<?> submit(final Session session) {
        final Future<?> sessionFuture = submit((Runnable) session);
        session.setSessionFuture(sessionFuture);
        return sessionFuture;
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(final Runnable runnable, final T value) {
        return new SessionFutureTask<>(runnable, value);
    }

    @Override
    protected void beforeExecute(final Thread t, final Runnable r) {
        if (r instanceof SessionFutureTask)
            ((SessionFutureTask<?>) r).getSession().ifPresent(rex -> rex.setSessionThread(t));
    }

    /**
     * A cancellable asynchronous operation with the added ability to get a {@link Session} instance if the
     * {@code Runnable} for the task was of that type.
     */
    public static class SessionFutureTask<V> extends FutureTask<V> {

        private final Session session;

        public SessionFutureTask(final Runnable runnable, final  V result) {
            super(runnable, result);

            // hold an instance to the Session instance if it is of that type
            this.session = runnable instanceof Session ? (Session) runnable : null;
        }

        public Optional<Session> getSession() {
            return Optional.ofNullable(this.session);
        }
    }
}
