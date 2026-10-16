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
package org.apache.tinkerpop.gremlin.tinkergraph.structure.storage;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;

import java.util.Collection;

/**
 * A pluggable durable storage engine for a transactional {@code TinkerStorageGraph}. Implementations persist the
 * changeset of each committed transaction to disk and rebuild the in-memory graph on open. The graph remains the
 * authoritative in-memory copy (write-through); the engine is a durable mirror.
 * <p/>
 * The lifecycle is:
 * <ol>
 *     <li>{@link #open(AbstractTinkerGraph, Configuration)} — resolve the backing location and ready the store.</li>
 *     <li>{@link #replay(AbstractTinkerGraph)} — rebuild in-memory state from what was previously persisted.</li>
 *     <li>{@link #persist(long, Collection, Collection)} then {@link #flush()} — called on each transaction commit,
 *     before the indexes and in-memory state are committed, so a failure aborts the commit and leaves the graph as it
 *     was. Whether a commit that failed in the engine reached disk is up to the engine; one that cannot say should
 *     refuse further commits until the graph is reopened.</li>
 *     <li>{@link #compact(AbstractTinkerGraph)} — optionally fold accumulated changes into a compact snapshot.</li>
 *     <li>{@link #close()} — release resources.</li>
 * </ol>
 * A new engine can be selected by name (see {@link DefaultStorage}) or by fully-qualified class name via the
 * {@code gremlin.tinkergraph.storage} configuration key. Implementations must provide a public no-argument constructor.
 */
public interface TinkerStorage extends AutoCloseable {

    /**
     * Prepare the storage engine for use, resolving its backing location from the supplied configuration. Called once
     * during graph construction before {@link #replay(AbstractTinkerGraph)}.
     *
     * @param graph  the graph that owns this engine
     * @param config the graph configuration, including {@code gremlin.tinkergraph.storage.directory}
     */
    void open(AbstractTinkerGraph graph, Configuration config);

    /**
     * Rebuild the in-memory state of the graph from previously persisted data. Called once during graph construction.
     * Implementations should re-apply persisted elements through the graph's own mutation API; the graph sets its
     * {@code loading} guard for the duration so this does not re-persist.
     *
     * @param graph the graph to populate
     */
    void replay(AbstractTinkerGraph graph);

    /**
     * Durably record the changeset of a committing transaction. Called from within the transaction commit, while the
     * changed elements are locked, before the in-memory commit is applied. Each mutation is either a put (added or
     * modified element) or a delete (see {@link TinkerStorageMutation}).
     *
     * @param txVersion       the version number of the committing transaction
     * @param changedVertices the vertex mutations in this transaction
     * @param changedEdges    the edge mutations in this transaction
     */
    void persist(long txVersion,
                 Collection<TinkerStorageMutation<TinkerVertex>> changedVertices,
                 Collection<TinkerStorageMutation<TinkerEdge>> changedEdges);

    /**
     * Force any buffered writes to durable storage. Called after {@link #persist(long, Collection, Collection)} as the
     * commit's durability point.
     */
    void flush();

    /**
     * Fold accumulated changes into a compact representation of the current committed state, reclaiming space. Safe to
     * call at any time; typically invoked on {@link #close()} or on a size/commit-count threshold.
     *
     * @param graph the graph whose current committed state should be snapshotted
     */
    void compact(AbstractTinkerGraph graph);

    /**
     * Compact if the engine's accumulated data has grown past its own threshold, otherwise do nothing. Called on the
     * commit path after {@link #flush()} (while the graph's commit lock is held) so a long-running graph that is never
     * explicitly closed does not grow its backing storage without bound. The default is a no-op, leaving compaction
     * entirely under the control of {@link #compact(AbstractTinkerGraph)} and {@link #close()}.
     *
     * @param graph the graph whose current committed state would be snapshotted
     */
    default void maybeCompact(final AbstractTinkerGraph graph) {
        // no-op by default; engines that accumulate an on-disk log override this to bound its growth
    }

    /**
     * Replace everything the engine holds with the graph's current committed state, which the caller has just
     * emptied, so that the store holds nothing and a reopen finds an empty graph. Called by
     * {@code TinkerGraph.clear()} under the graph's commit lock, and never on a read-only engine. If the store cannot
     * be emptied, the engine must refuse further commits until the graph is reopened: appending them to the old data
     * would bring the cleared elements back on reopen, mixed with the new ones. The default compacts, which is only
     * correct for an engine whose {@link #compact} throws when it cannot write, rather than returning quietly.
     *
     * @param graph the graph, already emptied, whose committed state the store should now hold
     */
    default void clear(final AbstractTinkerGraph graph) {
        compact(graph);
    }

    /**
     * Whether this engine accepts no writes at all, such as when a damaged store was opened for recovery. The graph
     * then also leaves its other files in the storage directory, such as index definitions, unchanged.
     */
    default boolean isReadOnly() {
        return false;
    }

    /**
     * Whether this engine has stopped accepting writes after a storage failure, such as a failed log write or the
     * store being removed while the graph was open. It stays so until the graph is reopened. The graph's committed
     * data remains readable in memory for export, so operations that would discard or rewrite it are refused.
     */
    default boolean isFailed() {
        return false;
    }

    /**
     * Stop accepting writes, as after a storage failure, until the graph is reopened. The graph calls this when a
     * transaction's changes were persisted and flushed but could not then be applied in memory: the transaction is
     * durable and returns on the next open, so the graph's memory no longer matches the store, and nothing written
     * from that memory, by a commit or a compaction, may reach the store. {@link #isFailed()} reports {@code true}
     * afterwards.
     *
     * @param cause why the graph's memory no longer matches the store
     */
    void failStop(Throwable cause);

    /**
     * {@inheritDoc}
     * <p/>
     * Flush and release resources. Does not delete persisted data.
     */
    @Override
    void close();
}
