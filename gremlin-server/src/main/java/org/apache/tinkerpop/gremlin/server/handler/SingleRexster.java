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

import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

/**
 * A simple {@link Rexster} implementation that accepts one request, processes it and exits.
 */
public class SingleRexster extends AbstractRexster {
    private static final Logger logger = LoggerFactory.getLogger(SingleRexster.class);
    protected final Context gremlinContext;

    SingleRexster(final Context gremlinContext, final String sessionId,
                  final ConcurrentMap<String, Rexster> sessions) {
        super(gremlinContext, sessionId,true, sessions);
        this.gremlinContext = gremlinContext;
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
    public boolean addTask(final Context gremlinContext) {
        throw new UnsupportedOperationException("SingleWorker doesn't accept tasks beyond the one provided to the constructor");
    }

    @Override
    public void run() {
        try {
            startTransaction(gremlinContext);
            process(gremlinContext);
        } catch (RexsterException we) {
            logger.warn(we.getMessage(), we);

            // should have already rolledback - this is a safety valve
            closeTransactionSafely(gremlinContext, Transaction.Status.ROLLBACK);

            gremlinContext.writeAndFlush(we.getResponseMessage());
        } finally {
            close();
        }
    }
}
