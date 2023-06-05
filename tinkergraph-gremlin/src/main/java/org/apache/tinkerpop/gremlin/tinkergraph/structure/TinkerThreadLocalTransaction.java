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
import java.util.Map;
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
    }

    @Override
    protected void doClose() {
        txNumber.set(NOT_STARTED);
    }

    protected long getTxNumber() {
        // todo: think a bit more...
        if (!isOpen()) txNumber.set(openedTx.getAndIncrement());
        return txNumber.get();
    }

    @Override
    protected void doCommit() throws TransactionException {
        final long txVersion = txNumber.get();

        // collect elements changed in tx
        final List<Map.Entry<Object, TinkerElementContainer<TinkerVertex>>> changedVertices =
                graph.getVertices().entrySet().stream().filter(v -> v.getValue().isChanged()).collect(Collectors.toList());
        final List<Map.Entry<Object, TinkerElementContainer<TinkerEdge>>> changedEdges =
                graph.getEdges().entrySet().stream().filter(v -> v.getValue().isChanged()).collect(Collectors.toList());

        try {
            // Double-checked locking to reduce lock time
            // todo: consider collection changes for each transaction in local list
            if (changedVertices.stream().anyMatch(v -> v.getValue().updatedOutsideTransaction()) ||
                    changedEdges.stream().anyMatch(v -> v.getValue().updatedOutsideTransaction()))
                throw new TransactionException(TX_CONFLICT);

            changedVertices.forEach(v -> {
                if (!v.getValue().tryLock()) throw new TransactionException(TX_CONFLICT);
            });
            changedEdges.forEach(e -> {
                if (!e.getValue().tryLock()) throw new TransactionException(TX_CONFLICT);
            });

            if (changedVertices.stream().anyMatch(v -> v.getValue().updatedOutsideTransaction()) ||
                    changedEdges.stream().anyMatch(e -> e.getValue().updatedOutsideTransaction()))
                throw new TransactionException(TX_CONFLICT);

            changedVertices.forEach(v -> v.getValue().commit(txVersion));
            changedEdges.forEach(e -> e.getValue().commit(txVersion));
        } catch (TransactionException ex) {
            changedVertices.forEach(v -> v.getValue().rollback());
            changedEdges.forEach(e -> e.getValue().rollback());

            throw ex;
        } finally {
            // remove elements from graph if not used in other tx's
            changedVertices.stream().filter(v -> v.getValue().canBeRemoved()).forEach(v -> graph.vertices.remove(v.getKey()));
            changedEdges.stream().filter(e -> e.getValue().canBeRemoved()).forEach(e -> graph.edges.remove(e.getKey()));

            changedVertices.forEach(v -> v.getValue().releaseLock());
            changedEdges.forEach(e -> e.getValue().releaseLock());
        }

        // todo: update indices ?

        doClose();
    }

    @Override
    protected void doRollback() throws TransactionException {
        if (!isOpen())
            throw new TransactionException(TX_CONFLICT);

        graph.getVertices().values().stream().filter(v -> v.isChanged()).forEach(v -> v.rollback());
        graph.getEdges().values().stream().filter(e -> e.isChanged()).forEach(e -> e.rollback());

        // todo: update indices ?

        doClose();
    }
}
