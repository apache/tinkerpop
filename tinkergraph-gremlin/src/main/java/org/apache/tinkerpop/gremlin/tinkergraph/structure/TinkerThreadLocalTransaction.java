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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

final class TinkerThreadLocalTransaction extends AbstractThreadLocalTransaction {

    // todo: rename and set messages
    private static final String TX_CONFLICT = "conflict message";

    private static final long NOT_STARTED = -1;

    private static final AtomicLong openedTx;
    private final ThreadLocal<Long> txNumber = ThreadLocal.withInitial(() -> NOT_STARTED);

    private final TinkerTransactionGraph graph;

    static {
        openedTx = new AtomicLong(0);
    }

    public TinkerThreadLocalTransaction(final TinkerTransactionGraph g) {
        super(g);
        graph = g;
    }

    @Override
    public boolean isOpen() {
        return txNumber.get() != NOT_STARTED;
    }

    @Override
    public <T extends TraversalSource> T begin() {
        doOpen();
        return super.begin();
    }

    @Override
    protected void doOpen() {
        if (isOpen())
            Transaction.Exceptions.transactionAlreadyOpen();
        txNumber.set(openedTx.getAndIncrement());
    }

    @Override
    protected void doClose() {
        txNumber.set(NOT_STARTED);
    }

    protected long getTxNumber() {
        return txNumber.get();
    }

    @Override
    protected void doCommit() throws TransactionException {
        if (!isOpen())
            throw new TransactionException(TX_CONFLICT);

        final List<TinkerElementContainer<TinkerVertex>> changedVertices =
                graph.getVertices().values().stream().filter(v -> v.isChanged()).collect(Collectors.toList());
        final List<TinkerElementContainer<TinkerEdge>> changedEdges =
                graph.getEdges().values().stream().filter(v -> v.isChanged()).collect(Collectors.toList());

        if (changedVertices.stream().anyMatch(v -> v.updatedOutsideTransaction()) ||
                changedEdges.stream().anyMatch(v -> v.updatedOutsideTransaction()))
            throw new TransactionException(TX_CONFLICT);

        try {
            changedVertices.forEach(v -> {
                if (!v.tryLock()) throw new TransactionException(TX_CONFLICT);
            });
            changedEdges.forEach(e -> {
                if (!e.tryLock()) throw new TransactionException(TX_CONFLICT);
            });

            if (changedVertices.stream().anyMatch(v -> v.updatedOutsideTransaction()) ||
                    changedEdges.stream().anyMatch(v -> v.updatedOutsideTransaction()))
                throw new TransactionException(TX_CONFLICT);

            changedVertices.forEach(v -> v.commit());
            changedEdges.forEach(e -> e.commit());
        } catch (TransactionException ex) {
            changedVertices.forEach(v -> v.rollback());
            changedEdges.forEach(e -> e.rollback());
        } finally {
            // todo: remove deleted elements from graph

            changedVertices.forEach(v -> v.releaseLock());
            changedEdges.forEach(e -> e.releaseLock());
        }

        // todo: update indexes

        doClose();
    }

    @Override
    protected void doRollback() throws TransactionException {
        if (!isOpen())
            throw new TransactionException(TX_CONFLICT);

        graph.getVertices().values().stream().filter(v -> v.isChanged()).forEach(v -> v.rollback());
        graph.getEdges().values().stream().filter(e -> e.isChanged()).forEach(e -> e.rollback());

        // todo: update indexes

        doClose();
    }
}
