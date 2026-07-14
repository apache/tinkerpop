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
 *     before the in-memory state is committed, so a failure aborts the commit and leaves memory and disk consistent.</li>
 *     <li>{@link #compact(AbstractTinkerGraph)} — optionally fold accumulated changes into a compact snapshot.</li>
 *     <li>{@link #close()} — release resources.</li>
 * </ol>
 * A new engine can be selected by name (see {@code TinkerStorageGraph.DefaultStorage}) or by fully-qualified class
 * name via the {@code gremlin.tinkergraph.storage} configuration key. Implementations must provide a public no-argument
 * constructor.
 */
public interface TinkerStorage extends AutoCloseable {

    /**
     * Prepare the storage engine for use, resolving its backing location from the supplied configuration. Called once
     * during graph construction before {@link #replay(AbstractTinkerGraph)}.
     *
     * @param graph  the graph that owns this engine
     * @param config the graph configuration, including {@code gremlin.tinkergraph.graphLocation}
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
     * {@inheritDoc}
     * <p/>
     * Flush and release resources. Does not delete persisted data.
     */
    @Override
    void close();
}
