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

import com.github.jelmerk.hnswlib.core.Item;
import com.github.jelmerk.hnswlib.core.SearchResult;
import com.github.jelmerk.hnswlib.core.hnsw.HnswIndex;
import com.github.jelmerk.hnswlib.core.Index;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A vector index implementation for TinkerGraph using hnswlib.
 *
 * @param <T> the element type (Vertex or Edge)
 */
final class TinkerVectorIndex<T extends Element> extends AbstractTinkerVectorIndex<T> {

    /**
     * Map of property key to vector index
     */
    protected Map<String, Index<Object, float[], ElementItem, Float>> vectorIndices = new ConcurrentHashMap<>();

    /**
     * Default M parameter for HNSW index
     */
    private static final int DEFAULT_M = 16;

    /**
     * Default ef construction parameter for HNSW index
     */
    private static final int DEFAULT_EF_CONSTRUCTION = 200;

    /**
     * Default ef parameter for HNSW index
     */
    private static final int DEFAULT_EF = 10;

    /**
     * Default maximum number of items in the index
     */
    private static final int DEFAULT_MAX_ITEMS = 100;

    /**
     * Configuration key for the dimension of the vector
     */
    public static final String CONFIG_DIMENSION = "dimension";

    /**
     * Configuration key for the M parameter of the HNSW index
     */
    public static final String CONFIG_M = "m";

    /**
     * Configuration key for the ef construction parameter of the HNSW index
     */
    public static final String CONFIG_EF_CONSTRUCTION = "efConstruction";

    /**
     * Configuration key for the ef parameter of the HNSW index
     */
    public static final String CONFIG_EF = "ef";

    /**
     * Configuration key for the maximum number of items in the index
     */
    public static final String CONFIG_MAX_ITEMS = "maxItems";

    /**
     * Configuration key for the distance function of the HNSW index
     */
    public static final String CONFIG_DISTANCE_FUNCTION = "distanceFunction";

    /**
     * Creates a new vector index for the specified graph and element class.
     *
     * @param graph      the graph
     * @param indexClass the element class
     */
    public TinkerVectorIndex(final TinkerGraph graph, final Class<T> indexClass) {
        super(graph, indexClass);
    }

    /**
     * Creates a vector index for the specified property key with the given configuration options.
     *
     * @param key           the property key
     * @param configuration the configuration options
     */
    @Override
    public void createIndex(final String key, final Map<String, Object> configuration) {
        if (null == key)
            throw Graph.Exceptions.argumentCanNotBeNull("key");
        if (key.isEmpty())
            throw new IllegalArgumentException("The key for the index cannot be an empty string");

        // Get dimension from configuration or throw exception if not provided
        if (!configuration.containsKey(CONFIG_DIMENSION))
            throw new IllegalArgumentException("The dimension must be provided in the configuration");

        final int dimension;
        final Object dimObj = configuration.get(CONFIG_DIMENSION);
        if (dimObj instanceof Number) {
            dimension = ((Number) dimObj).intValue();
        } else {
            throw new IllegalArgumentException("The dimension must be a number");
        }

        if (dimension <= 0)
            throw new IllegalArgumentException("The dimension must be greater than 0");

        if (this.indexedKeys.contains(key))
            return;
        this.indexedKeys.add(key);

        int m = DEFAULT_M;
        if (configuration.containsKey(CONFIG_M)) {
            final Object mObj = configuration.get(CONFIG_M);
            if (mObj instanceof Number) {
                m = ((Number) mObj).intValue();
            }
        }

        int efConstruction = DEFAULT_EF_CONSTRUCTION;
        if (configuration.containsKey(CONFIG_EF_CONSTRUCTION)) {
            final Object efObj = configuration.get(CONFIG_EF_CONSTRUCTION);
            if (efObj instanceof Number) {
                efConstruction = ((Number) efObj).intValue();
            }
        }

        int ef = DEFAULT_EF;
        if (configuration.containsKey(CONFIG_EF)) {
            final Object efObj = configuration.get(CONFIG_EF);
            if (efObj instanceof Number) {
                ef = ((Number) efObj).intValue();
            }
        }

        int maxItems = DEFAULT_MAX_ITEMS;
        if (configuration.containsKey(CONFIG_MAX_ITEMS)) {
            final Object maxObj = configuration.get(CONFIG_MAX_ITEMS);
            if (maxObj instanceof Number) {
                maxItems = ((Number) maxObj).intValue();
            }
        }

        TinkerIndexType.Vector vector = TinkerIndexType.Vector.COSINE;
        if (configuration.containsKey(CONFIG_DISTANCE_FUNCTION)) {
            final Object vec = configuration.get(CONFIG_DISTANCE_FUNCTION);
            if (vec instanceof TinkerIndexType.Vector) {
                vector = ((TinkerIndexType.Vector) vec);
            }
        }

        // Create a new HNSW index for this property key
        final Index<Object, float[], ElementItem, Float> index = HnswIndex
                .newBuilder(dimension, vector.getDistanceFunction(), Float::compare, maxItems)
                .withM(m)
                .withEfConstruction(efConstruction)
                .withEf(ef)
                .withRemoveEnabled()
                .build();

        this.vectorIndices.put(key, index);

        // Index existing elements
        (Vertex.class.isAssignableFrom(this.indexClass) ?
                ((TinkerGraph) this.graph).vertices.values().parallelStream() :
                ((TinkerGraph) this.graph).edges.values().parallelStream())
                .map(e -> new Object[]{((T) e).property(key), e})
                .filter(a -> ((Property) a[0]).isPresent())
                .forEach(a -> {
                    // values for the key that don't match the dimensions of the index won't be added
                    final Object value = ((Property) a[0]).value();
                    if (value instanceof float[] && ((float[]) value).length == dimension) {
                        this.addToIndex(key, (float[]) value, (T) a[1]);
                    }
                });
    }

    /**
     * Adds an element with a vector to the index.
     *
     * @param key     the property key
     * @param vector  the vector
     * @param element the element
     */
    public void addToIndex(final String key, final float[] vector, final T element) {
        if (!this.indexedKeys.contains(key) || !this.vectorIndices.containsKey(key))
            return;

        final Index<Object, float[], ElementItem, Float> index = this.vectorIndices.get(key);
        final ElementItem item = new ElementItem(element.id(), vector, element);
        index.add(item);
    }

    /**
     * Searches for nearest neighbors in the vector index.
     *
     * @param key    the property key
     * @param vector the query vector
     * @param k      the number of nearest neighbors to return
     * @return a list of elements sorted by distance
     */
    public List<TinkerIndexElement<T>> findNearest(final String key, final float[] vector, final int k) {
        if (!this.indexedKeys.contains(key) || !this.vectorIndices.containsKey(key))
            throw new IllegalArgumentException("The key '" + key + "' is not indexed");

        final Index<Object, float[], ElementItem, Float> index = this.vectorIndices.get(key);
        final List<SearchResult<ElementItem, Float>> nearest = index.findNearest(vector, k);
        return nearest.stream().map(sr ->
                new TinkerIndexElement<>(sr.item().element, sr.distance())).collect(Collectors.toList());
    }

    /**
     * Searches for nearest neighbors in the vector index.
     *
     * @param key    the property key
     * @param vector the query vector
     * @param k      the number of nearest neighbors to return
     * @return a list of elements sorted by distance
     */
    public List<T> findNearestElements(final String key, final float[] vector, final int k) {
        if (!this.indexedKeys.contains(key) || !this.vectorIndices.containsKey(key))
            throw new IllegalArgumentException("The key '" + key + "' is not indexed");

        final Index<Object, float[], ElementItem, Float> index = this.vectorIndices.get(key);
        final List<SearchResult<ElementItem, Float>> nearest = index.findNearest(vector, k);
        return nearest.stream().map(sr -> sr.item().element).collect(Collectors.toList());
    }

    /**
     * Removes an element from the vector index.
     *
     * @param key     the property key
     * @param element the element
     */
    public void removeFromIndex(final String key, final T element) {
        if (!this.indexedKeys.contains(key) || !this.vectorIndices.containsKey(key))
            return;

        final Index<Object, float[], ElementItem, Float> index = this.vectorIndices.get(key);
        try {
            index.remove(element.id(), 0);
        } catch (Exception e) {
            // If the element is not in the index, just ignore the exception
        }
    }

    /**
     * Updates the vector index when an element's property changes.
     *
     * @param key      the property key
     * @param newValue the new vector value
     * @param element  the element
     */
    public void updateIndex(final String key, final float[] newValue, final T element) {
        if (!this.indexedKeys.contains(key) || !this.vectorIndices.containsKey(key))
            return;

        final Index<Object, float[], ElementItem, Float> index = this.vectorIndices.get(key);
        try {
            index.remove(element.id(), 0);
        } catch (Exception e) {
            // If the element is not in the index, just ignore the exception
        }
        final ElementItem item = new ElementItem(element.id(), newValue, element);
        index.add(item);
    }

    /**
     * Drops the vector index for the specified property key.
     *
     * @param key the property key
     */
    @Override
    public void dropIndex(final String key) {
        if (this.vectorIndices.containsKey(key)) {
            this.vectorIndices.remove(key);
        }

        this.indexedKeys.remove(key);
    }

    /**
     * A class that wraps an element with its vector for use in the HNSW index.
     */
    private class ElementItem implements Item<Object, float[]>, Serializable {
        private final Object id;
        private final float[] vector;
        private final T element;

        public ElementItem(final Object id, final float[] vector, final T element) {
            this.id = id;
            this.vector = vector;
            this.element = element;
        }

        @Override
        public Object id() {
            return id;
        }

        @Override
        public float[] vector() {
            return vector;
        }

        @Override
        public int dimensions() {
            return vector.length;
        }
    }

    // AbstractTinkerIndex implementation methods

    @Override
    public List<T> get(final String key, final Object value) {
        // This method is for regular indices, not vector indices
        return Collections.emptyList();
    }

    @Override
    public long count(final String key, final Object value) {
        // This method is for regular indices, not vector indices
        return 0;
    }

    @Override
    public void remove(final String key, final Object value, final T element) {
        // For vector indices, we use removeFromIndex
        if (value instanceof float[]) {
            removeFromIndex(key, element);
        }
    }

    @Override
    public void removeElement(final T element) {
        if (this.indexClass.isAssignableFrom(element.getClass())) {
            for (String key : this.indexedKeys) {
                removeFromIndex(key, element);
            }
        }
    }

    @Override
    public void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {
        if (this.indexedKeys.contains(key) && newValue instanceof float[]) {
            updateIndex(key, (float[]) newValue, element);
        }
    }
}
