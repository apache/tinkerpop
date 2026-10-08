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
package org.apache.tinkerpop.gremlin.structure.snapshot.graph;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SnapshotLayout;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.strategy.CsrNativeStrategy;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A read-only {@link Graph} over a {@code FULL} {@link CsrSnapshot}. Elements are flyweight facades over ordinals that
 * are created on demand and never cached. The graph does not support mutation or transactions, and its variables are
 * read-only. Facades must not be used after {@link #close()}.
 */
public final class CsrGraph implements Graph {

    static {
        TraversalStrategies.GlobalCache.registerStrategies(CsrGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()
                        .addStrategies(CsrNativeStrategy.instance()));
    }

    /**
     * The configuration key of the snapshot directory.
     */
    public static final String GREMLIN_CSR_DIRECTORY = "gremlin.csr.directory";

    /**
     * The configuration key that selects checksum verification when the snapshot is opened; defaults to false.
     */
    public static final String GREMLIN_CSR_VERIFY_CHECKSUMS = "gremlin.csr.verifyChecksums";

    private final CsrSnapshot snapshot;
    private final SharedSnapshot sharedSnapshot;
    private final Configuration configuration;
    private final Features features;
    private final Variables variables;
    private final Map<String, CsrComputePropertyColumn> computeProperties;
    private final boolean edgesVisible;
    private final AtomicBoolean closed = new AtomicBoolean();

    private final String[] vertexKeys;
    private final String[] edgeKeys;
    private final Map<String, Integer> vertexKeyCodes;
    private final Map<String, Integer> edgeKeyCodes;
    private final ColumnReader[] edgeColumns;

    private CsrGraph(final CsrSnapshot snapshot, final Configuration configuration) {
        this(new SharedSnapshot(snapshot), configuration, java.util.Collections.emptyMap(), true);
    }

    private CsrGraph(final SharedSnapshot sharedSnapshot, final Configuration configuration,
                     final Map<String, CsrComputePropertyColumn> computeProperties, final boolean edgesVisible) {
        this.sharedSnapshot = sharedSnapshot;
        this.snapshot = sharedSnapshot.snapshot();
        this.configuration = configuration;
        this.features = new CsrFeatures(snapshot);
        this.variables = new CsrVariables(snapshot.variables());
        this.computeProperties = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(computeProperties));
        this.edgesVisible = edgesVisible;

        final List<String> vKeys = snapshot.vertexPropertyKeys();
        this.vertexKeys = vKeys.toArray(new String[0]);
        this.vertexKeyCodes = new HashMap<>();
        for (int i = 0; i < vertexKeys.length; i++) {
            vertexKeyCodes.put(vertexKeys[i], i);
        }

        final List<String> eKeys = snapshot.edgePropertyKeys();
        this.edgeKeys = eKeys.toArray(new String[0]);
        this.edgeKeyCodes = new HashMap<>();
        this.edgeColumns = new ColumnReader[edgeKeys.length];
        for (int i = 0; i < edgeKeys.length; i++) {
            edgeKeyCodes.put(edgeKeys[i], i);
            edgeColumns[i] = snapshot.edgeProperty(edgeKeys[i]);
        }
    }

    /**
     * Opens the snapshot in the named directory without checksum verification.
     */
    public static CsrGraph open(final String directory) {
        return open(Paths.get(directory));
    }

    /**
     * Opens the snapshot in the directory without checksum verification.
     */
    public static CsrGraph open(final Path directory) {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(GREMLIN_CSR_DIRECTORY, directory.toString());
        return open(directory, false, conf);
    }

    /**
     * Opens the snapshot named by {@code gremlin.csr.directory}, verifying checksums if
     * {@code gremlin.csr.verifyChecksums} is true. This is the {@code GraphFactory} entry point.
     */
    public static CsrGraph open(final Configuration configuration) {
        final String directory = configuration.getString(GREMLIN_CSR_DIRECTORY, null);
        if (directory == null) {
            throw new IllegalArgumentException("The configuration must set " + GREMLIN_CSR_DIRECTORY);
        }
        return open(Paths.get(directory), configuration.getBoolean(GREMLIN_CSR_VERIFY_CHECKSUMS, false), configuration);
    }

    private static CsrGraph open(final Path directory, final boolean verifyChecksums, final Configuration configuration) {
        final CsrSnapshot snapshot = CsrSnapshot.open(directory, verifyChecksums);
        if (snapshot.layout() != SnapshotLayout.FULL) {
            final SnapshotLayout layout = snapshot.layout();
            snapshot.close();
            throw new IllegalArgumentException("CsrGraph requires a FULL snapshot but " + directory + " has layout "
                    + layout);
        }
        try {
            return new CsrGraph(snapshot, configuration);
        } catch (final RuntimeException | Error e) {
            snapshot.close();
            throw e;
        }
    }

    /**
     * The underlying snapshot.
     */
    public CsrSnapshot snapshot() {
        return snapshot;
    }

    // ---------------------------------------------------------------- code lookups and facade factories

    /**
     * The facade of the vertex with the ordinal.
     */
    public CsrVertex vertexAt(final int ordinal) {
        return new CsrVertex(this, ordinal);
    }

    /**
     * The facade of the edge with the ordinal.
     */
    public CsrEdge edgeAt(final int ordinal) {
        return new CsrEdge(this, ordinal);
    }

    /**
     * The facade of a vertex property, given its owner, key code and vertex-property ordinal.
     */
    public <V> CsrVertexProperty<V> vertexPropertyAt(final int vertex, final int keyCode, final long propertyOrdinal) {
        return new CsrVertexProperty<>(this, vertex, keyCode, propertyOrdinal);
    }

    /**
     * The facade of an edge property, given its edge and key code. It may be absent, see {@code Property#isPresent}.
     */
    public <V> CsrProperty<V> edgePropertyAt(final int edge, final int keyCode) {
        return new CsrProperty<>(this, edge, keyCode);
    }

    /**
     * The facade of a meta-property, given its vertex property and meta key code.
     */
    public <V> CsrMetaProperty<V> metaPropertyAt(final int vertex, final int keyCode, final long propertyOrdinal,
                                                 final int metaKeyCode) {
        return new CsrMetaProperty<>(this, vertex, keyCode, propertyOrdinal, metaKeyCode);
    }

    /**
     * The code of the vertex property key, or -1 if the snapshot has none.
     */
    public int vertexKeyCode(final String key) {
        final Integer code = vertexKeyCodes.get(key);
        return code == null ? -1 : code;
    }

    public int edgeKeyCode(final String key) {
        final Integer code = edgeKeyCodes.get(key);
        return code == null ? -1 : code;
    }

    public int vertexKeyCount() {
        return vertexKeys.length;
    }

    public int edgeKeyCount() {
        return edgeKeys.length;
    }

    public String vertexKey(final int code) {
        return vertexKeys[code];
    }

    public String edgeKey(final int code) {
        return edgeKeys[code];
    }

    public int metaKeyCode(final String key) {
        return snapshot.metaKeyCodeOf(key);
    }

    public String metaKey(final int code) {
        return snapshot.metaPropertyKeys().get(code);
    }

    public ColumnReader edgeColumn(final int code) {
        return edgeColumns[code];
    }

    CsrComputePropertyColumn computeProperty(final String key) {
        return computeProperties.get(key);
    }

    Set<String> computePropertyKeys() {
        return computeProperties.keySet();
    }

    Map<String, CsrComputePropertyColumn> computeProperties() {
        return computeProperties;
    }

    boolean edgesVisible() {
        return edgesVisible;
    }

    CsrGraph resultView(final Map<String, CsrComputePropertyColumn> properties, final boolean includeEdges) {
        sharedSnapshot.retain();
        try {
            return new CsrGraph(sharedSnapshot, configuration, properties, includeEdges);
        } catch (final RuntimeException | Error e) {
            sharedSnapshot.release();
            throw e;
        }
    }

    void retainSnapshot() {
        sharedSnapshot.retain();
    }

    void releaseSnapshot() {
        sharedSnapshot.release();
    }

    // ---------------------------------------------------------------- Graph

    @Override
    public Vertex addVertex(final Object... keyValues) {
        throw Graph.Exceptions.vertexAdditionsNotSupported();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        if (!graphComputerClass.equals(CsrGraphComputer.class))
            throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass);
        return (C) new CsrGraphComputer(this);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return new CsrGraphComputer(this);
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        if (vertexIds.length == 0) return new VertexScan();
        final int[] ordinals = new int[vertexIds.length];
        int n = 0;
        for (final Object id : vertexIds) {
            if (id == null) continue;
            final int ordinal = snapshot.vertexOrdinalCoerced(id instanceof Element ? ((Element) id).id() : id);
            if (ordinal >= 0) ordinals[n++] = ordinal;
        }
        return new VertexOrdinals(ordinals, n);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        if (!edgesVisible) return java.util.Collections.emptyIterator();
        if (edgeIds.length == 0) return new EdgeScan();
        final int[] ordinals = new int[edgeIds.length];
        int n = 0;
        for (final Object id : edgeIds) {
            if (id == null) continue;
            final int ordinal = snapshot.edgeOrdinalCoerced(id instanceof Element ? ((Element) id).id() : id);
            if (ordinal >= 0) ordinals[n++] = ordinal;
        }
        return new EdgeOrdinals(ordinals, n);
    }

    @Override
    public Transaction tx() {
        throw Graph.Exceptions.transactionsNotSupported();
    }

    @Override
    public Variables variables() {
        return variables;
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public Features features() {
        return features;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) sharedSnapshot.release();
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "vertices:" + snapshot.vertexCount() + " edges:" + snapshot.edgeCount());
    }

    // ---------------------------------------------------------------- iterators

    private final class VertexScan implements Iterator<Vertex> {
        private final int count = snapshot.vertexCount();
        private int next = 0;

        @Override
        public boolean hasNext() {
            return next < count;
        }

        @Override
        public Vertex next() {
            if (next >= count) throw new NoSuchElementException();
            return new CsrVertex(CsrGraph.this, next++);
        }
    }

    private final class EdgeScan implements Iterator<Edge> {
        private final int count = snapshot.edgeCount();
        private int next = 0;

        @Override
        public boolean hasNext() {
            return next < count;
        }

        @Override
        public Edge next() {
            if (next >= count) throw new NoSuchElementException();
            return new CsrEdge(CsrGraph.this, next++);
        }
    }

    private final class VertexOrdinals implements Iterator<Vertex> {
        private final int[] ordinals;
        private final int size;
        private int next = 0;

        private VertexOrdinals(final int[] ordinals, final int size) {
            this.ordinals = ordinals;
            this.size = size;
        }

        @Override
        public boolean hasNext() {
            return next < size;
        }

        @Override
        public Vertex next() {
            if (next >= size) throw new NoSuchElementException();
            return new CsrVertex(CsrGraph.this, ordinals[next++]);
        }
    }

    private final class EdgeOrdinals implements Iterator<Edge> {
        private final int[] ordinals;
        private final int size;
        private int next = 0;

        private EdgeOrdinals(final int[] ordinals, final int size) {
            this.ordinals = ordinals;
            this.size = size;
        }

        @Override
        public boolean hasNext() {
            return next < size;
        }

        @Override
        public Edge next() {
            if (next >= size) throw new NoSuchElementException();
            return new CsrEdge(CsrGraph.this, ordinals[next++]);
        }
    }

    // ---------------------------------------------------------------- variables

    /**
     * The read-only graph variables of the snapshot. A variable whose value is null is a key with no value, because
     * {@link Variables#get(String)} cannot return it.
     */
    private static final class CsrVariables implements Variables {

        private final Map<String, Object> values;

        private CsrVariables(final Map<String, Object> values) {
            this.values = values;
        }

        @Override
        public Set<String> keys() {
            return values.keySet();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> Optional<R> get(final String key) {
            return Optional.ofNullable((R) values.get(key));
        }

        @Override
        public Map<String, Object> asMap() {
            return java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(values));
        }

        @Override
        public void set(final String key, final Object value) {
            throw new UnsupportedOperationException("The variables of a snapshot graph are read-only");
        }

        @Override
        public void remove(final String key) {
            throw new UnsupportedOperationException("The variables of a snapshot graph are read-only");
        }

        @Override
        public String toString() {
            return StringFactory.graphVariablesString(this);
        }
    }

    /**
     * Keeps the mapped snapshot alive while source, result and in-flight computer views share it.
     */
    private static final class SharedSnapshot {
        private final CsrSnapshot snapshot;
        private final AtomicInteger references = new AtomicInteger(1);

        private SharedSnapshot(final CsrSnapshot snapshot) {
            this.snapshot = snapshot;
        }

        private CsrSnapshot snapshot() {
            return snapshot;
        }

        private SharedSnapshot retain() {
            while (true) {
                final int current = references.get();
                if (current == 0)
                    throw new IllegalStateException("The CsrGraph snapshot is already closed");
                if (current == Integer.MAX_VALUE)
                    throw new IllegalStateException("Too many CsrGraph snapshot references");
                if (references.compareAndSet(current, current + 1)) return this;
            }
        }

        private void release() {
            final int remaining = references.decrementAndGet();
            if (remaining == 0) {
                snapshot.close();
            } else if (remaining < 0) {
                references.incrementAndGet();
                throw new IllegalStateException("The CsrGraph snapshot was released too many times");
            }
        }
    }
}
