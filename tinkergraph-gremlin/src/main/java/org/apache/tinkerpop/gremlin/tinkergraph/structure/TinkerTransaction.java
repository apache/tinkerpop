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
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

final class TinkerTransaction extends AbstractThreadLocalTransaction {

    private static final String TX_CONFLICT = "Conflict: element modified in another transaction";

    private static final long NOT_STARTED = -1;

    private static final AtomicLong openedTx;
    private final ThreadLocal<Long> txNumber = ThreadLocal.withInitial(() -> NOT_STARTED);
    private final ThreadLocal<Set<TinkerElementContainer<TinkerVertex>>> txChangedVertices = new ThreadLocal<>();
    private final ThreadLocal<Set<TinkerElementContainer<TinkerEdge>>> txChangedEdges = new ThreadLocal<>();

    private final TinkerTransactionGraph graph;

    static {
        openedTx = new AtomicLong(0);
    }

    public TinkerTransaction(final TinkerTransactionGraph g) {
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

    protected long getTxNumber() {
        // todo: think a bit more...
        if (!isOpen()) txNumber.set(openedTx.getAndIncrement());
        return txNumber.get();
    }

    protected <T extends TinkerElement> void touch(TinkerElementContainer<T> container) {
        if (!isOpen()) txNumber.set(openedTx.getAndIncrement());

        T element = container.get();
        if (null == element) element = container.getUnmodified();
        if (element instanceof TinkerVertex) {
            if (null == txChangedVertices.get())
                txChangedVertices.set(new HashSet<>());
            txChangedVertices.get().add((TinkerElementContainer<TinkerVertex>) container);
        } else {
            if (null == txChangedEdges.get())
                txChangedEdges.set(new HashSet<>());
            txChangedEdges.get().add((TinkerElementContainer<TinkerEdge>) container);
        }
    }

    @Override
    protected void doCommit() throws TransactionException {
        final long txVersion = txNumber.get();

        Set<TinkerElementContainer<TinkerVertex>> changedVertices = txChangedVertices.get();
        if (null == changedVertices) changedVertices = Collections.emptySet();
        Set<TinkerElementContainer<TinkerEdge>> changedEdges = txChangedEdges.get();
        if (null == changedEdges) changedEdges = Collections.emptySet();

        try {
            // Double-checked locking to reduce lock time
            if (changedVertices.stream().anyMatch(v -> v.updatedOutsideTransaction()) ||
                    changedEdges.stream().anyMatch(v -> v.updatedOutsideTransaction()))
                throw new TransactionException(TX_CONFLICT);

            changedVertices.forEach(v -> {
                if (!v.tryLock()) throw new TransactionException(TX_CONFLICT);
            });
            changedEdges.forEach(e -> {
                if (!e.tryLock()) throw new TransactionException(TX_CONFLICT);
            });

            if (changedVertices.stream().anyMatch(v -> v.updatedOutsideTransaction()) ||
                    changedEdges.stream().anyMatch(e -> e.updatedOutsideTransaction()))
                throw new TransactionException(TX_CONFLICT);

            final TinkerTransactionalIndex vertexIndex = (TinkerTransactionalIndex) graph.vertexIndex;
            if (vertexIndex != null) vertexIndex.commit(changedVertices);
            final TinkerTransactionalIndex edgeIndex = (TinkerTransactionalIndex) graph.edgeIndex;
            if (edgeIndex != null) edgeIndex.commit(changedEdges);

            changedVertices.forEach(v -> v.commit(txVersion));
            changedEdges.forEach(e -> e.commit(txVersion));
        } catch (TransactionException ex) {
            changedVertices.forEach(v -> v.rollback());
            changedEdges.forEach(e -> e.rollback());

            final TinkerTransactionalIndex vertexIndex = (TinkerTransactionalIndex) graph.vertexIndex;
            if (vertexIndex != null) vertexIndex.rollback();
            final TinkerTransactionalIndex edgeIndex = (TinkerTransactionalIndex) graph.edgeIndex;
            if (edgeIndex != null) edgeIndex.rollback();

            throw ex;
        } finally {
            // remove elements from graph if not used in other tx's
            changedVertices.stream().filter(v -> v.canBeRemoved()).forEach(v -> graph.vertices.remove(v.getElementId()));
            changedEdges.stream().filter(e -> e.canBeRemoved()).forEach(e -> graph.edges.remove(e.getElementId()));

            txChangedVertices.remove();
            txChangedEdges.remove();

            changedVertices.forEach(v -> v.releaseLock());
            changedEdges.forEach(e -> e.releaseLock());

            txNumber.set(NOT_STARTED);
        }
    }

    @Override
    protected void doRollback() throws TransactionException {
        Set<TinkerElementContainer<TinkerVertex>> changedVertices = txChangedVertices.get();
        if (null != changedVertices) changedVertices.forEach(v -> v.rollback());
        Set<TinkerElementContainer<TinkerEdge>> changedEdges = txChangedEdges.get();
        if (null != changedEdges) changedEdges.forEach(e -> e.rollback());

        final TinkerTransactionalIndex vertexIndex = (TinkerTransactionalIndex) graph.vertexIndex;
        if (vertexIndex != null) vertexIndex.rollback();
        final TinkerTransactionalIndex edgeIndex = (TinkerTransactionalIndex) graph.edgeIndex;
        if (vertexIndex != null) edgeIndex.rollback();

        txChangedVertices.remove();
        txChangedEdges.remove();

        txNumber.set(NOT_STARTED);
    }
}