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

import static org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerVectorSearchFactory.Params.KEY;
import static org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerVectorSearchFactory.Params.TOP_K;
import static org.apache.tinkerpop.gremlin.util.CollectionUtil.asMap;

/**
 * Service to utilize a {@code TinkerVertexIndex} to do a vector search.
 */
public class TinkerVectorSearchFactory extends TinkerServiceRegistry.TinkerServiceFactory<Element, Map<String, Object>> implements Service<Element, Map<String, Object>> {

    public static final String NAME = "tinker.search.vector.topKByElement";

    public interface Params {
        /**
         * Specify the key storing the embedding
         */
        String KEY = "key";
        /**
         * Number of results to return
         */
        String TOP_K = "topK";

        Map DESCRIBE = asMap(
                KEY, "Specify they key storing the embedding for the vector search",
                TOP_K, "Number of results to return (optional, defaults to 10)"
        );
    }

    public TinkerVectorSearchFactory(final AbstractTinkerGraph graph) {
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

        // add 1 because we always filter 1 out of the index because it will return a match on itself
        final int k = (int) params.getOrDefault(TOP_K, 10) + 1;
        final Element e = in.get();

        // if the current element does not have the specified key, then return no results
        if (!e.keys().contains(key))
            return CloseableIterator.empty();

        final float[] embedding = e.value(key);
        if (e instanceof Vertex) {
            return CloseableIterator.of(graph.findNearestVertices(key, embedding, k).stream()
                    .filter(tie -> !tie.getElement().equals(e))
                    .map(TinkerIndexElement::toMap).iterator());
        } else if (e instanceof Edge) {
            return CloseableIterator.of(graph.findNearestEdges(key, embedding, k).stream()
                    .filter(tie -> !tie.getElement().equals(e))
                    .map(TinkerIndexElement::toMap).iterator());
        } else {
            return CloseableIterator.empty();
        }
    }

    @Override
    public void close() {}

}

