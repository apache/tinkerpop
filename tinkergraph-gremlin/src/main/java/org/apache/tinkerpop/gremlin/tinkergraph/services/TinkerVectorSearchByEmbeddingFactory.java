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
package org.apache.tinkerpop.gremlin.tinkergraph.services;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIndexElement;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerVectorSearchByElementFactory.Params.KEY;
import static org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerVectorSearchByElementFactory.Params.TOP_K;
import static org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerVectorSearchByEmbeddingFactory.Params.ELEMENT;
import static org.apache.tinkerpop.gremlin.util.CollectionUtil.asMap;

/**
 * Service to utilize a {@code TinkerVertexIndex} to do a vector search using a specified embedding.
 */
public class TinkerVectorSearchByEmbeddingFactory extends TinkerServiceRegistry.TinkerServiceFactory<Float[], Map<String, Object>> implements Service<Float[], Map<String, Object>> {

    public static final String NAME = "tinker.search.vector.topKByEmbedding";

    public interface Params {
        /**
         * Specify the key storing the embedding
         */
        String KEY = "key";
        /**
         * Number of results to return
         */
        String TOP_K = "topK";
        /**
         * Specify whether the search should be for a "vertex" or "edge"
         */
        String ELEMENT = "element";

        Map DESCRIBE = asMap(
                KEY, "Specify they key storing the embedding for the vector search",
                TOP_K, "Number of results to return (optional, defaults to 10)",
                ELEMENT, "Specify whether the search should be for a \"vertex\" or \"edge\""
        );
    }

    public TinkerVectorSearchByEmbeddingFactory(final AbstractTinkerGraph graph) {
        super(graph, NAME);
    }

    @Override
    public Type getType() {
        return Type.Streaming;
    }

    @Override
    public Map describeParams() {
        return Params.DESCRIBE;
    }

    @Override
    public Set<Type> getSupportedTypes() {
        return Collections.singleton(Type.Streaming);
    }

    @Override
    public Service<Float[], Map<String, Object>> createService(final boolean isStart, final Map params) {
        if (isStart) {
            throw new UnsupportedOperationException(Exceptions.cannotStartTraversal);
        }

        if (!params.containsKey(KEY)) {
            throw new IllegalArgumentException("The parameter map must contain the key specifying where the embedding is: " + KEY);
        }

        if (!params.containsKey(ELEMENT) || !Arrays.asList("vertex", "edge").contains(params.get(ELEMENT))) {
            throw new IllegalArgumentException("The parameter map must contain a key: " + ELEMENT + " with a value of \"vertex\" or \"edge\"");
        }

        return this;
    }

    @Override
    public CloseableIterator<Map<String,Object>> execute(final ServiceCallContext ctx, final Traverser.Admin<Float[]> in, final Map params) {
        final String key = (String) params.get(KEY);
        final int k = (int) params.getOrDefault(TOP_K, 10);
        final Object traverserObject = in.get();

        final float[] embedding = traverserObject instanceof Float[] ?
                ArrayUtils.toPrimitive((Float[]) traverserObject) : (float[]) traverserObject;

        final String elementType = (String) params.get(ELEMENT);
        if ("vertex".equals(elementType)) {
            return CloseableIterator.of(graph.findNearestVertices(key, embedding, k).stream()
                    .map(TinkerIndexElement::toMap).iterator());
        } else if ("edge".equals(elementType)) {
            return CloseableIterator.of(graph.findNearestEdges(key, embedding, k).stream()
                    .map(TinkerIndexElement::toMap).iterator());
        } else {
            return CloseableIterator.empty();
        }
    }

    @Override
    public void close() {}

}
