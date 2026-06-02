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

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIndexElement;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerVectorSearchByElementFactory.Params.FILTER;
import static org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerVectorSearchByElementFactory.Params.KEY;
import static org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerVectorSearchByElementFactory.Params.TOP_K;
import static org.apache.tinkerpop.gremlin.util.CollectionUtil.asMap;

/**
 * Service to utilize a {@code TinkerVertexIndex} to do a vector search using an embedding from a supplied vertex
 * or edge.
 */
public class TinkerVectorSearchByElementFactory extends TinkerServiceRegistry.TinkerServiceFactory<Element, Map<String, Object>> implements Service<Element, Map<String, Object>> {

    public static final String NAME = "tinker.search.vector.topK.byElement";

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
         * Optional map of property key to required value restricting which candidates are searched.
         * Use the special key {@code "~label"} to filter by element label.
         */
        String FILTER = "filter";

        Map DESCRIBE = asMap(
                KEY, "Specify they key storing the embedding for the vector search",
                TOP_K, "Number of results to return (optional, defaults to 10)",
                FILTER, "Map of property key to required value to restrict candidates (optional); use \"~label\" to filter by label"
        );
    }

    public TinkerVectorSearchByElementFactory(final AbstractTinkerGraph graph) {
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
    public Service<Element, Map<String, Object>> createService(final boolean isStart, final Map params) {
        if (isStart) {
            throw new UnsupportedOperationException(Exceptions.cannotStartTraversal);
        }

        if (!params.containsKey(KEY)) {
            throw new IllegalArgumentException("The parameter map must contain the key where the embedding is: " + KEY);
        }

        return this;
    }

    @Override
    public CloseableIterator<Map<String,Object>> execute(final ServiceCallContext ctx, final Traverser.Admin<Element> in, final Map params) {
        final String key = (String) params.get(KEY);
        final int k = (int) params.getOrDefault(TOP_K, 10);
        final Map<String, Object> filter = (Map<String, Object>) params.get(FILTER);
        final Element e = in.get();

        if (!e.keys().contains(key))
            return CloseableIterator.empty();

        final float[] embedding = e.value(key);
        if (e instanceof Vertex) {
            return CloseableIterator.of(graph.findNearestVertices(key, embedding, k, filter, e.id()).stream()
                    .map(TinkerIndexElement::toMap).iterator());
        } else if (e instanceof Edge) {
            return CloseableIterator.of(graph.findNearestEdges(key, embedding, k, filter, e.id()).stream()
                    .map(TinkerIndexElement::toMap).iterator());
        } else {
            return CloseableIterator.empty();
        }
    }

    @Override
    public void close() {}

}

