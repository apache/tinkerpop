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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.CardinalityValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import static java.util.stream.Collectors.toList;

/**
 * Implementation for the {@code mergeV()} step covering both the start step version and the one used mid-traversal.
 */
public class MergeVertexStep<S> extends MergeStep<S, Vertex, Map> {

    private static final Set allowedTokens = new LinkedHashSet(Arrays.asList(T.id, T.label));

    public static void validateMapInput(final Map map, final boolean ignoreTokens) {
        MergeStep.validate(map, ignoreTokens, allowedTokens, "mergeV");
    }


    public MergeVertexStep(final Traversal.Admin traversal, final boolean isStart) {
        super(traversal, isStart);
    }

    public MergeVertexStep(final Traversal.Admin traversal, final boolean isStart, final Map merge) {
        super(traversal, isStart, merge);
    }

    public MergeVertexStep(final Traversal.Admin traversal, final boolean isStart, final Traversal.Admin<S,Map> mergeTraversal) {
        super(traversal, isStart, mergeTraversal);
    }

    @Override
    public MergeVertexStep<S> clone() {
        return (MergeVertexStep<S>) super.clone();
    }

    @Override
    protected Set getAllowedTokens() {
        return allowedTokens;
    }

    @Override
    protected Iterator<Vertex> flatMap(final Traverser.Admin<S> traverser) {
        final Graph graph = getGraph();

        final Map mergeMap = materializeMap(traverser, mergeTraversal);
        validateMapInput(mergeMap, false);

        Iterator<Vertex> vertices = searchVertices(mergeMap);

        if (onMatchTraversal != null) {
            // attach the onMatch properties
            vertices = IteratorUtils.peek(vertices, v -> {

                // if this was a start step the traverser is initialized with Boolean/false, so override that with
                // the matched Vertex so that the option() traversal can operate on it properly
                if (isStart) traverser.set((S) v);

                // assume good input from GraphTraversal - folks might drop in a T here even though it is immutable
                final Map<String, Object> onMatchMap = materializeMap(traverser, onMatchTraversal);
                validateMapInput(onMatchMap, true);

                onMatchMap.forEach((key, value) -> {
                    Object val = value;
                    VertexProperty.Cardinality card = graph.features().vertex().getCardinality(key);

                    // a value can be a traversal in the case where the user specifies the cardinality for the value.
                    if (value instanceof CardinalityValueTraversal) {
                        final CardinalityValueTraversal cardinalityValueTraversal =  (CardinalityValueTraversal) value;
                        card = cardinalityValueTraversal.getCardinality();
                        val = cardinalityValueTraversal.getValue();
                    }

                    // trigger callbacks for eventing - in this case, it's a VertexPropertyChangedEvent. if there's no
                    // registry/callbacks then just set the property
                    if (this.callbackRegistry != null && !callbackRegistry.getCallbacks().isEmpty()) {
                        final EventStrategy eventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class).get();
                        final Property<?> p = v.property(key);
                        final Property<Object> oldValue = p.isPresent() ? eventStrategy.detach(v.property(key)) : null;
                        final Event.VertexPropertyChangedEvent vpce = new Event.VertexPropertyChangedEvent(eventStrategy.detach(v), oldValue, val);
                        this.callbackRegistry.getCallbacks().forEach(c -> c.accept(vpce));
                    }

                    // try to detect proper cardinality for the key according to the graph
                    v.property(card, key, val);
                });
            });
        }

        /*
         * Search produced results, and onMatch action will be triggered.
         */
        if (vertices.hasNext()) {
            return vertices;
        }

        // make sure we close the search traversal
        CloseableIterator.closeIterator(vertices);

        /*
         * This onCreateMap will inherit from the main merge argument - a union of merge and onCreate with no overrides
         * allowed.
         */
        final Map<?,?> onCreateMap = onCreateMap(traverser, mergeMap);

        final Object[] flatArgs = onCreateMap.entrySet().stream()
                .flatMap(e -> Stream.of(e.getKey(), e.getValue())).collect(toList()).toArray();

        final Vertex vertex = graph.addVertex(flatArgs);

        // trigger callbacks for eventing - in this case, it's a VertexAddedEvent
        if (this.callbackRegistry != null && !callbackRegistry.getCallbacks().isEmpty()) {
            final EventStrategy eventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class).get();
            final Event.VertexAddedEvent vae = new Event.VertexAddedEvent(eventStrategy.detach(vertex));
            this.callbackRegistry.getCallbacks().forEach(c -> c.accept(vae));
        }

        return IteratorUtils.of(vertex);
    }

    /**
     * Fuse the mergeMap with any additional key/values from the onCreateTraversal. No overrides allowed.
     */
    protected Map onCreateMap(final Traverser.Admin<S> traverser, final Map mergeMap) {
        // no onCreateTraversal - use main mergeMap argument
        if (onCreateTraversal == null)
            return mergeMap;

        final Map onCreateMap = materializeMap(traverser, onCreateTraversal);
        // null result from onCreateTraversal - use main mergeMap argument
        if (onCreateMap == null || onCreateMap.size() == 0)
            return mergeMap;
        validateMapInput(onCreateMap, false);

        if (mergeMap == null || mergeMap.size() == 0)
            return onCreateMap;

        /*
         * Now we need to merge the two maps - onCreate should inherit traits from mergeMap, and it is not allowed to
         * override values for any keys.
         */

        validateNoOverrides(mergeMap, onCreateMap);

        final Map<Object, Object> combinedMap = new HashMap<>(onCreateMap.size() + mergeMap.size());
        combinedMap.putAll(onCreateMap);
        combinedMap.putAll(mergeMap);

        return combinedMap;
    }

}
