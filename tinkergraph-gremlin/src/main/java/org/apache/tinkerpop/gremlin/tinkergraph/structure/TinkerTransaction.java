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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.storage.TinkerStorageMutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of {@link AbstractThreadLocalTransaction} for {@link TinkerStorageGraph}
 */
final class TinkerTransaction extends AbstractThreadLocalTransaction {

    private static final Logger logger = LoggerFactory.getLogger(TinkerTransaction.class);

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

    private final TinkerStorageGraph graph;

    static {
        openedTx = new AtomicLong(0);
    }

    public TinkerTransaction(final TinkerStorageGraph g) {
        super(g);
        graph = g;
    }

    @Override
    public boolean isOpen() {
        return txNumber.get() != NOT_STARTED;
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
     * 5. persist the changes when a storage engine is configured
     * 6. update indices
     * 7. commit all changes
     * On {@link TransactionException} or any other failure before the changes are applied (such as a storage write):
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

        // set once the changes are durable, after which a failure cannot undo the transaction, only keep it from
        // being seen until the graph is reopened
        boolean recorded = false;
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

            // write-ahead: durably persist the changeset before applying the in-memory commit. A failure here aborts
            // the commit and the catch below rolls back memory. If the failure was in writing or
            // flushing the log, the engine also stops accepting commits until the graph is reopened, because what
            // reached disk is then unknown: the failed transaction may or may not be in the log, and reopening
            // replays only what is actually there. Skipped while the graph is replaying its storage log on open.
            // Serialized by storageCommitLock because commits of disjoint elements otherwise reach the engine's single
            // append log concurrently and interleave its records.
            //
            // The in-memory apply is held inside that same lock. Compaction builds its snapshot by reading the graph
            // and then discards the log, which is only sound while the graph reflects everything the log holds. That
            // is false for exactly as long as a changeset sits persisted but not yet applied, so a compaction landing
            // in that window snapshots without the transaction and then deletes the record that held it, losing an
            // acknowledged commit. Publishing under the lock closes the window, and also makes the order in which
            // transactions become visible match the order they were recorded in.
            //
            // A transaction that changed nothing has nothing to record, so it neither takes the lock nor touches the
            // engine. Every read opens a transaction that is later committed, so writing for it would make each read
            // append and sync a frame, and would fail reads on a graph whose storage refuses commits: one opened
            // to recover a damaged store, or one stopped after a storage failure, whose data must stay exportable.
            final boolean durable = graph.storage != null && !graph.loading &&
                    !(changedVertices.isEmpty() && changedEdges.isEmpty());
            if (durable) graph.storageCommitLock.lock();
            try {
                if (durable) {
                    // clear() empties the graph under this lock and marks every element container removed, so a commit
                    // that passed the checks above while waiting here must look again before it writes anything
                    if (changedVertices.stream().anyMatch(v -> v.updatedOutsideTransaction()) ||
                            changedEdges.stream().anyMatch(e -> e.updatedOutsideTransaction()))
                        throw new TransactionException(TX_CONFLICT);
                    graph.storage.persist(txVersion, toVertexMutations(changedVertices), toEdgeMutations(changedEdges));
                    graph.storage.flush();
                    recorded = true;
                }

                // update indices only once the storage write has succeeded. Committing an index changes the shared
                // index that every transaction reads, and rollback can only discard this transaction's own index
                // changes, so an index committed ahead of a storage write that then failed would keep the failed
                // transaction's view until the graph is reopened.
                final TinkerTransactionalIndex vertexIndex = (TinkerTransactionalIndex) graph.vertexIndex;
                if (vertexIndex != null) vertexIndex.commit(changedVertices);
                final TinkerTransactionalIndex edgeIndex = (TinkerTransactionalIndex) graph.edgeIndex;
                if (edgeIndex != null) edgeIndex.commit(changedEdges);

                // commit all changes
                changedVertices.forEach(v -> v.commit(txVersion));
                changedEdges.forEach(e -> e.commit(txVersion));

                // bound log growth for a long-running graph that is never explicitly closed; no-op unless the
                // engine's accumulated log has crossed its threshold. Runs after the apply so the snapshot it may
                // write includes this transaction rather than omitting it and then truncating the log that held it.
                // The transaction is already applied and durable by now, so a failed compaction must not fail it.
                // Compaction is crash-safe (the old snapshot and log stay intact) and is tried again after later
                // commits.
                if (durable) {
                    try {
                        graph.storage.maybeCompact(graph);
                    } catch (RuntimeException ex) {
                        if (graph.storage.isFailed())
                            logger.error("Storage compaction failed after a successful commit and the storage now " +
                                    "accepts no further commits until the graph is reopened", ex);
                        else
                            logger.warn("Storage compaction failed after a successful commit; it will be retried", ex);
                    }
                }
            } finally {
                if (durable) graph.storageCommitLock.unlock();
            }
        } catch (RuntimeException | Error ex) {
            // rollback on error
            changedVertices.forEach(v -> v.rollback());
            changedEdges.forEach(e -> e.rollback());

            // also revert indices update
            final TinkerTransactionalIndex vertexIndex = (TinkerTransactionalIndex) graph.vertexIndex;
            if (vertexIndex != null) vertexIndex.rollback();
            final TinkerTransactionalIndex edgeIndex = (TinkerTransactionalIndex) graph.edgeIndex;
            if (edgeIndex != null) edgeIndex.rollback();

            // the transaction is committed once it is durable, as a database counts it once its commit record is in
            // the log, so a failure applying it afterwards is not reported as a rollback. Memory keeps the state from
            // before the transaction, so the storage must take nothing more from it, and reopening shows the commit.
            if (recorded) {
                graph.storage.failStop(ex);
                throw new TransactionException(String.format("The transaction was recorded durably in storage at %s " +
                        "but could not be applied in memory, so it will be present when the graph is reopened. No " +
                        "further commits are accepted until then", graph.storageDirectory), ex);
            }
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
     * Convert the changed vertex containers into the storage-facing {@link TinkerStorageMutation} view. Called during
     * commit, before {@code commit()} is applied to the containers, so the modified value is still available via
     * {@link TinkerElementContainer#getModified()}.
     */
    private static List<TinkerStorageMutation<TinkerVertex>> toVertexMutations(final Set<TinkerElementContainer<TinkerVertex>> changed) {
        final List<TinkerStorageMutation<TinkerVertex>> mutations = new ArrayList<>(changed.size());
        for (final TinkerElementContainer<TinkerVertex> c : changed) {
            mutations.add(c.isDeleted()
                    ? new TinkerStorageMutation<>(c.getElementId(), null)
                    : new TinkerStorageMutation<>(c.getElementId(), c.getModified()));
        }
        return mutations;
    }

    /**
     * Convert the changed edge containers into the storage-facing {@link TinkerStorageMutation} view.
     */
    private static List<TinkerStorageMutation<TinkerEdge>> toEdgeMutations(final Set<TinkerElementContainer<TinkerEdge>> changed) {
        final List<TinkerStorageMutation<TinkerEdge>> mutations = new ArrayList<>(changed.size());
        for (final TinkerElementContainer<TinkerEdge> c : changed) {
            mutations.add(c.isDeleted()
                    ? new TinkerStorageMutation<>(c.getElementId(), null)
                    : new TinkerStorageMutation<>(c.getElementId(), c.getModified()));
        }
        return mutations;
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
        if (edgeIndex != null) edgeIndex.rollback();

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
