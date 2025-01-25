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

/**
 * Implementation of {@link AbstractThreadLocalTransaction} for {@link TinkerTransactionGraph}
 */
final class TinkerTransaction extends AbstractThreadLocalTransaction {

    private static final String TX_CONFLICT = "Conflict: element modified in another transaction";

    /**
     * Initial value of transaction number.
     */
    private static final long NOT_STARTED = -1;

    /**
     * Counter for opened transactions. Used to get unique id for each new transaction.
     */
    private static final AtomicLong openedTx;

    /**
     * Unique number of transaction for each thread.
     */
    private final ThreadLocal<Long> txNumber = ThreadLocal.withInitial(() -> NOT_STARTED);

    /**
     * Set of references to vertex containers changed in current transaction.
     */
    private final ThreadLocal<Set<TinkerElementContainer<TinkerVertex>>> txChangedVertices = new ThreadLocal<>();

    /**
     * Set of references to edge containers changed in current transaction.
     */
    private final ThreadLocal<Set<TinkerElementContainer<TinkerEdge>>> txChangedEdges = new ThreadLocal<>();

    /**
     * Set of references to element containers read in current transaction.
     */
    private final ThreadLocal<Set<TinkerElementContainer>> txReadElements = new ThreadLocal<>();

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
        txNumber.set(openedTx.getAndIncrement());
    }

    protected long getTxNumber() {
        if (!isOpen()) txNumber.set(openedTx.getAndIncrement());
        return txNumber.get();
    }

    /**
     * Adds element to list of changes in current transaction.
     */
    protected <T extends TinkerElement> void markChanged(TinkerElementContainer<T> container) {
        if (!isOpen()) txNumber.set(openedTx.getAndIncrement());

        T element = container.getUnmodified();
        if (null == element) element = container.getModified();
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

    /**
     * Adds element to list of read in current transaction.
     */
    protected <T extends TinkerElement> void markRead(TinkerElementContainer container) {
        if (!isOpen()) txNumber.set(openedTx.getAndIncrement());

        if (null == txReadElements.get())
            txReadElements.set(new HashSet<>());

        txReadElements.get().add(container);
    }

    /**
     * Try to commit all changes made in current transaction.
     * Workflow:
     * 1. collect all changes
     * 2. verify if any elements already changed, throw {@link TransactionException} if any
     * 3. try to lock all containers to prevent other tx from making changes
     * 4. one more time verify elements versions
     * 5. update indices
     * 6. commit all changes
     * On {@link TransactionException}:
     *  rollback all changes
     * Lastly:
     *  cleanup transaction intermediate variables.
     *
     * @throws TransactionException
     */
    @Override
    protected void doCommit() throws TransactionException {
        final long txVersion = txNumber.get();

        // collect all changes
        Set<TinkerElementContainer<TinkerVertex>> changedVertices = txChangedVertices.get();
        if (null == changedVertices) changedVertices = Collections.emptySet();
        Set<TinkerElementContainer<TinkerEdge>> changedEdges = txChangedEdges.get();
        if (null == changedEdges) changedEdges = Collections.emptySet();

        try {
            // Double-checked locking to reduce lock time
            if (changedVertices.stream().anyMatch(v -> v.updatedOutsideTransaction()) ||
                    changedEdges.stream().anyMatch(v -> v.updatedOutsideTransaction()))
                throw new TransactionException(TX_CONFLICT);

            // try to lock all element containers, throw exception if any element already locked by other tx
            changedVertices.forEach(v -> {
                if (!v.tryLock())
                    throw new TransactionException(TX_CONFLICT);
            });
            changedEdges.forEach(e -> {
                if (!e.tryLock())
                    throw new TransactionException(TX_CONFLICT);
            });

            // verify versions of all elements to be sure no element changes during setting lock
            if (changedVertices.stream().anyMatch(v -> v.updatedOutsideTransaction()) ||
                    changedEdges.stream().anyMatch(e -> e.updatedOutsideTransaction()))
                throw new TransactionException(TX_CONFLICT);

            // update indices
            final TinkerTransactionalIndex vertexIndex = (TinkerTransactionalIndex) graph.vertexIndex;
            if (vertexIndex != null) vertexIndex.commit(changedVertices);
            final TinkerTransactionalIndex edgeIndex = (TinkerTransactionalIndex) graph.edgeIndex;
            if (edgeIndex != null) edgeIndex.commit(changedEdges);

            // commit all changes
            changedVertices.forEach(v -> v.commit(txVersion));
            changedEdges.forEach(e -> e.commit(txVersion));
        } catch (TransactionException ex) {
            // rollback on error
            changedVertices.forEach(v -> v.rollback());
            changedEdges.forEach(e -> e.rollback());

            // also revert indices update
            final TinkerTransactionalIndex vertexIndex = (TinkerTransactionalIndex) graph.vertexIndex;
            if (vertexIndex != null) vertexIndex.rollback();
            final TinkerTransactionalIndex edgeIndex = (TinkerTransactionalIndex) graph.edgeIndex;
            if (edgeIndex != null) edgeIndex.rollback();

            throw ex;
        } finally {
            // remove elements from graph if not used in other tx's
            changedVertices.stream().filter(v -> v.canBeRemoved()).forEach(v -> graph.getVertices().remove(v.getElementId()));
            changedEdges.stream().filter(e -> e.canBeRemoved()).forEach(e -> graph.getEdges().remove(e.getElementId()));

            final Set<TinkerElementContainer> readElements = txReadElements.get();
            if (readElements != null)
                readElements.stream().forEach(e -> e.commit(txVersion));

            txChangedVertices.remove();
            txChangedEdges.remove();
            txReadElements.remove();

            changedVertices.forEach(v -> v.releaseLock());
            changedEdges.forEach(e -> e.releaseLock());

            txNumber.set(NOT_STARTED);
        }
    }

    /**
     * Rollback all changes made in current transaction.
     * Workflow:
     * 1. Rollback all changes. Lock is not needed here because only this thread have access to data.
     * 2. Rollback indices changes, should be safe.
     * 3. Cleanup transaction intermediate variables.
     *
     * @throws TransactionException
     */
    @Override
    protected void doRollback() throws TransactionException {
        // rollback for all changed elements
        Set<TinkerElementContainer<TinkerVertex>> changedVertices = txChangedVertices.get();
        if (null != changedVertices) changedVertices.forEach(v -> v.rollback());
        Set<TinkerElementContainer<TinkerEdge>> changedEdges = txChangedEdges.get();
        if (null != changedEdges) changedEdges.forEach(e -> e.rollback());

        // rollback indices
        final TinkerTransactionalIndex vertexIndex = (TinkerTransactionalIndex) graph.vertexIndex;
        if (vertexIndex != null) vertexIndex.rollback();
        final TinkerTransactionalIndex edgeIndex = (TinkerTransactionalIndex) graph.edgeIndex;
        if (vertexIndex != null) edgeIndex.rollback();

        // cleanup unused containers
        if (null != changedVertices)
            changedVertices.stream().filter(v -> v.canBeRemoved()).forEach(v -> graph.getVertices().remove(v.getElementId()));
        if (null != changedEdges)
            changedEdges.stream().filter(e -> e.canBeRemoved()).forEach(e -> graph.getEdges().remove(e.getElementId()));

        final Set<TinkerElementContainer> readElements = txReadElements.get();
        if (readElements != null)
            readElements.stream().forEach(e -> e.reset());

        txChangedVertices.remove();
        txChangedEdges.remove();
        txReadElements.remove();

        txNumber.set(NOT_STARTED);
    }
}
