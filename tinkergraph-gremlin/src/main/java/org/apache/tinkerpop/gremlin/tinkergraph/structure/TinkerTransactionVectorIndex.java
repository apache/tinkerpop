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

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A vector index implementation for TinkerTransactionGraph using JVector.
 *
 * @param <T> the element type (Vertex or Edge)
 */
final class TinkerTransactionVectorIndex<T extends TinkerElement> extends AbstractTinkerVectorIndex<T> {

    private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();

    protected Map<String, IndexState<T>> vectorIndices = new ConcurrentHashMap<>();

    TinkerTransactionVectorIndex(final TinkerTransactionGraph graph, final Class<T> indexClass) {
        super(graph, indexClass);
    }

    @Override
    public void createIndex(final String key, final Map<String, Object> configuration) {
        if (null == key)
            throw Graph.Exceptions.argumentCanNotBeNull("key");
        if (key.isEmpty())
            throw new IllegalArgumentException("The key for the index cannot be an empty string");

        if (!configuration.containsKey(CONFIG_DIMENSION))
            throw new IllegalArgumentException("The dimension must be provided in the configuration");

        final Object dimObj = configuration.get(CONFIG_DIMENSION);
        if (!(dimObj instanceof Number))
            throw new IllegalArgumentException("The dimension must be a number");
        final int dimension = ((Number) dimObj).intValue();
        if (dimension <= 0)
            throw new IllegalArgumentException("The dimension must be greater than 0");

        if (this.indexedKeys.contains(key))
            return;
        this.indexedKeys.add(key);

        int m = DEFAULT_M;
        if (configuration.containsKey(CONFIG_M) && configuration.get(CONFIG_M) instanceof Number)
            m = ((Number) configuration.get(CONFIG_M)).intValue();

        int efConstruction = DEFAULT_EF_CONSTRUCTION;
        if (configuration.containsKey(CONFIG_EF_CONSTRUCTION) && configuration.get(CONFIG_EF_CONSTRUCTION) instanceof Number)
            efConstruction = ((Number) configuration.get(CONFIG_EF_CONSTRUCTION)).intValue();

        TinkerIndexType.Vector distFunc = TinkerIndexType.Vector.COSINE;
        if (configuration.containsKey(CONFIG_DISTANCE_FUNCTION) && configuration.get(CONFIG_DISTANCE_FUNCTION) instanceof TinkerIndexType.Vector)
            distFunc = (TinkerIndexType.Vector) configuration.get(CONFIG_DISTANCE_FUNCTION);

        final IndexState<T> state = new IndexState<>(dimension, m, efConstruction, distFunc.toJVectorFunction());
        this.vectorIndices.put(key, state);

        final Map<?, ?> elementMap =
                Vertex.class.isAssignableFrom(indexClass) ?
                        ((TinkerTransactionGraph) graph).getVertices() :
                        ((TinkerTransactionGraph) graph).getEdges();

        for (final Object raw : elementMap.values()) {
            final TinkerElementContainer container = (TinkerElementContainer) raw;
            final Object e = container.get();
            if (e != null && indexClass.isInstance(e)) {
                final T element = (T) e;
                final Property<?> prop = element.property(key);
                if (prop.isPresent() && prop.value() instanceof float[]) {
                    final float[] v = (float[]) prop.value();
                    if (v.length == dimension)
                        state.add(element, v);
                }
            }
        }
    }

    public void addToIndex(final String key, final float[] vector, final T element) {
        final IndexState<T> state = this.vectorIndices.get(key);
        if (state == null)
            return;
        state.add(element, vector);
    }

    @Override
    public List<TinkerIndexElement<T>> findNearest(final String key, final float[] vector, final int k) {
        final IndexState<T> state = this.vectorIndices.get(key);
        if (state == null)
            throw new IllegalArgumentException("The key '" + key + "' is not indexed");
        return state.search(vector, k);
    }

    @Override
    public List<T> findNearestElements(final String key, final float[] vector, final int k) {
        return findNearest(key, vector, k).stream()
                .map(TinkerIndexElement::getElement)
                .collect(Collectors.toList());
    }

    public void removeFromIndex(final String key, final T element) {
        final IndexState<T> state = this.vectorIndices.get(key);
        if (state == null)
            return;
        state.remove(element);
    }

    public void updateIndex(final String key, final float[] newValue, final T element) {
        final IndexState<T> state = this.vectorIndices.get(key);
        if (state == null)
            return;
        state.remove(element);
        state.add(element, newValue);
    }

    @Override
    public void dropIndex(final String key) {
        final IndexState<T> state = this.vectorIndices.remove(key);
        if (state != null)
            state.close();
        this.indexedKeys.remove(key);
    }

    @Override
    public List<T> get(final String key, final Object value) {
        return Collections.emptyList();
    }

    @Override
    public long count(final String key, final Object value) {
        return 0;
    }

    @Override
    public void remove(final String key, final Object value, final T element) {
        // index changes only applied on tx commit
    }

    @Override
    public void removeElement(final T element) {
        // index changes only applied on tx commit
    }

    @Override
    public void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {
        // index changes only applied on tx commit
    }

    public void commit(final Set<TinkerElementContainer> updatedElements) {
        for (final TinkerElementContainer container : updatedElements) {
            final Object element = container.get();
            if (element != null && !container.isDeleted() && indexClass.isInstance(element)) {
                final T tinkerElement = (T) element;
                for (final String key : this.indexedKeys) {
                    final Property<?> prop = tinkerElement.property(key);
                    if (prop.isPresent() && prop.value() instanceof float[])
                        updateIndex(key, (float[]) prop.value(), tinkerElement);
                }
            } else if (container.isDeleted()) {
                final Object oldElement = container.getUnmodified();
                if (oldElement != null && indexClass.isInstance(oldElement)) {
                    final T tinkerOldElement = (T) oldElement;
                    for (final String key : this.indexedKeys)
                        removeFromIndex(key, tinkerOldElement);
                }
            }
        }
    }

    public void rollback() {
        // no action needed; index changes are deferred to commit
    }

    /**
     * Per-key index state backed by JVector.
     */
    private static final class IndexState<T extends TinkerElement> {
        private final int dimension;
        private final VectorSimilarityFunction similarityFunction;
        private final List<VectorFloat<?>> vectors = new ArrayList<>();
        private final List<T> elements = new ArrayList<>();
        private final Map<Object, Integer> idToOrdinal = new ConcurrentHashMap<>();
        private final GraphIndexBuilder builder;

        IndexState(final int dimension, final int m, final int efConstruction,
                   final VectorSimilarityFunction similarityFunction) {
            this.dimension = dimension;
            this.similarityFunction = similarityFunction;
            final ListRandomAccessVectorValues ravv = new ListRandomAccessVectorValues(vectors, dimension);
            this.builder = new GraphIndexBuilder(ravv, similarityFunction, m, efConstruction, 1.4f, 1.2f);
        }

        synchronized void add(final T element, final float[] vector) {
            if (vector.length != dimension)
                throw new IllegalArgumentException(
                        "Vector dimension " + vector.length + " does not match index dimension " + dimension);
            final int ordinal = vectors.size();
            final VectorFloat<?> vf = VTS.createFloatVector(vector);
            vectors.add(vf);
            elements.add(element);
            idToOrdinal.put(element.id(), ordinal);
            builder.addGraphNode(ordinal, vf);
        }

        synchronized void remove(final T element) {
            final Integer ordinal = idToOrdinal.remove(element.id());
            if (ordinal != null)
                builder.markNodeDeleted(ordinal);
        }

        List<TinkerIndexElement<T>> search(final float[] queryVector, final int k) {
            if (vectors.isEmpty())
                return Collections.emptyList();
            final VectorFloat<?> query = VTS.createFloatVector(queryVector);
            final ListRandomAccessVectorValues ravv = new ListRandomAccessVectorValues(vectors, dimension);
            final var ssp = SearchScoreProvider.exact(query, similarityFunction, ravv);
            try (final GraphSearcher searcher = new GraphSearcher(builder.getGraph())) {
                final SearchResult result = searcher.search(ssp, k, io.github.jbellis.jvector.util.Bits.ALL);
                return Arrays.stream(result.getNodes())
                        .map(ns -> new TinkerIndexElement<>(elements.get(ns.node), 1.0f - ns.score))
                        .collect(Collectors.toList());
            } catch (Exception e) {
                throw new RuntimeException("Vector search failed", e);
            }
        }

        void close() {
            try {
                builder.close();
            } catch (Exception ignored) {}
        }
    }
}
