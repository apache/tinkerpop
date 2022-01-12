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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeVertexStep;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Optimizes {@code mergeV()} searches by attempting to use an index where possible.
 */
public class TinkerMergeVertexStep<S> extends MergeVertexStep<S> {
    public TinkerMergeVertexStep(final MergeVertexStep step) {
        super(step.getTraversal(), step.isStart(), step.getSearchCreateTraversal());
        if (step.getOnMatchTraversal() != null) this.addChildOption(Merge.onMatch, step.getOnMatchTraversal());
        if (step.getOnCreateTraversal() != null) this.addChildOption(Merge.onCreate, step.getOnCreateTraversal());
        if (step.getCallbackRegistry() != null) this.callbackRegistry = step.getCallbackRegistry();
    }

    @Override
    protected Stream<Vertex> createSearchStream(final Map<Object, Object> search) {
        final TinkerGraph graph = (TinkerGraph) this.getTraversal().getGraph().get();
        Optional<String> firstIndex = Optional.empty();

        Stream<Vertex> stream;
        // prioritize lookup by id but otherwise attempt an index lookup
        if (search.containsKey(T.id)) {
            stream = IteratorUtils.stream(graph.vertices(search.get(T.id)));
        } else {
            // look for the first index we can find - that's the lucky winner. may or may not be the most selective
            final Set<String> indexedKeys = graph.getIndexedKeys(Vertex.class);
            firstIndex = search.keySet().stream().
                    filter(k -> k instanceof String).
                    map(k -> (String) k).
                    filter(indexedKeys::contains).findFirst();

            // use the index if possible otherwise just in memory filter
            stream = firstIndex.map(s -> TinkerHelper.queryVertexIndex(graph, s, search.get(s)).stream().map(v -> (Vertex) v)).
                    orElseGet(() -> IteratorUtils.stream(graph.vertices()));
        }

        final Optional<String> indexUsed = firstIndex;
        stream = stream.filter(v -> {
            // try to match on all search criteria skipping T.id as it was handled above
            return search.entrySet().stream().filter(kv -> {
                final Object k = kv.getKey();
                return k != T.id && !(indexUsed.isPresent() && indexUsed.get().equals(k));
            }).allMatch(kv -> {
                if (kv.getKey() == T.label) {
                    return v.label().equals(kv.getValue());
                } else {
                    final VertexProperty<Object> vp = v.property(kv.getKey().toString());
                    return vp.isPresent() && kv.getValue().equals(vp.value());
                }
            });
        });

        return stream;
    }
}
