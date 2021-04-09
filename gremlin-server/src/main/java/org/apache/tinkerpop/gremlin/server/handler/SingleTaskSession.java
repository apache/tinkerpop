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

import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;

/**
 * A simple {@link Session} implementation that accepts one request, processes it and exits.
 */
public class SingleTaskSession extends AbstractSession {
    private static final Logger logger = LoggerFactory.getLogger(SingleTaskSession.class);
    protected final SessionTask onlySessionTask;

    SingleTaskSession(final SessionTask onlySessionTask, final String sessionId,
                      final ConcurrentMap<String, Session> sessions) {
        super(onlySessionTask, sessionId,true, sessions);
        this.onlySessionTask = onlySessionTask;
    }

    /**
     * The {@code SingleWorker} can only process one request so the initial construction of it already has the
     * request in it and no more can be added, therefore this method always return {@code false}.
     */
    @Override
    public boolean isAcceptingTasks() {
        return false;
    }

    @Override
    public boolean submitTask(final SessionTask sessionTask) {
        throw new UnsupportedOperationException("SingleWorker doesn't accept tasks beyond the one provided to the constructor");
    }

    @Override
    public void run() {
        // allow the Session to know about the thread that is running it - the thread really only has relevance
        // once the session has started.
        this.sessionThread = Thread.currentThread();

        try {
            startTransaction(onlySessionTask);
            process(onlySessionTask);
        } catch (SessionException we) {
            // if the close reason isn't already set then things stopped during gremlin execution somewhere and not
            // more external issues like channel close or timeouts.
            closeReason.compareAndSet(null, CloseReason.PROCESSING_EXCEPTION);

            logger.warn(we.getMessage(), we);

            // should have already rolledback - this is a safety valve
            closeTransactionSafely(onlySessionTask, Transaction.Status.ROLLBACK);

            onlySessionTask.writeAndFlush(we.getResponseMessage());
        } finally {
            closeReason.compareAndSet(null, CloseReason.EXIT_PROCESSING);
            close();
        }
    }
}
