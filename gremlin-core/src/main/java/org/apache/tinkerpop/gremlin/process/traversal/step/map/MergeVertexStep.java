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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

/**
 * Implementation for the {@code mergeV()} step covering both the start step version and the one used mid-traversal.
 */
public class MergeVertexStep<S> extends MergeStep<S, Vertex, Map> {

    private static final Set allowedTokens = new LinkedHashSet(Arrays.asList(T.id, T.label));

    public static void validateMapInput(final Map map, final boolean ignoreTokens) {
        MergeStep.validate(map, ignoreTokens, allowedTokens);
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

    public static Iterator<Vertex> searchVertices(final Graph graph, final Map search) {
        if (search == null)
            return Collections.emptyIterator();

        final Object id = search.get(T.id);
        final String label = (String) search.get(T.label);

        GraphTraversal t;
        if (id != null)
            t = graph.traversal().V(id);
        else
            t = graph.traversal().V();
        if (label != null)
            t = t.hasLabel(label);

        // add property constraints
        for (final Map.Entry e : ((Map<?,?>) search).entrySet()) {
            final Object k = e.getKey();
            if (!(k instanceof String)) continue;
            t = t.has((String) k, e.getValue());
        }

        // this should auto-close the underlying traversal
        return t;
    }

    /**
     * Use the {@code Map} of search criteria to most efficiently return a {@code Stream<Vertex>} of matching elements.
     * Providers might override this method when extending this step to provide their own optimized mechanisms for
     * matching the list of vertices. This implementation is only optimized for the {@link T#id} so any other usage
     * will simply be in-memory filtering which could be slow.
     */
    protected Iterator<Vertex> searchVertices(final Map search) {
        return searchVertices(getGraph(), search);
    }

    @Override
    protected Iterator<Vertex> flatMap(final Traverser.Admin<S> traverser) {
        final Graph graph = getGraph();

        final Map mergeMap = materializeMap(traverser, mergeTraversal);
        validateMapInput(mergeMap, false);

        Iterator<Vertex> vertices = searchVertices(mergeMap);

        if (onMatchTraversal != null) {

            vertices = IteratorUtils.peek(vertices, v -> {

                // if this was a start step the traverser is initialized with Boolean/false, so override that with
                // the matched Vertex so that the option() traversal can operate on it properly
                if (isStart) traverser.set((S) v);

                // assume good input from GraphTraversal - folks might drop in a T here even though it is immutable
                final Map<String, Object> onMatchMap = materializeMap(traverser, onMatchTraversal);
                validateMapInput(onMatchMap, true);

                onMatchMap.forEach((key, value) -> {
                    // trigger callbacks for eventing - in this case, it's a VertexPropertyChangedEvent. if there's no
                    // registry/callbacks then just set the property
                    if (this.callbackRegistry != null && !callbackRegistry.getCallbacks().isEmpty()) {
                        final EventStrategy eventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class).get();
                        final Property<?> p = v.property(key);
                        final Property<Object> oldValue = p.isPresent() ? eventStrategy.detach(v.property(key)) : null;
                        final Event.VertexPropertyChangedEvent vpce = new Event.VertexPropertyChangedEvent(eventStrategy.detach(v), oldValue, value);
                        this.callbackRegistry.getCallbacks().forEach(c -> c.accept(vpce));
                    }

                    // try to detect proper cardinality for the key according to the graph
                    v.property(graph.features().vertex().getCardinality(key), key, value);
                });
            });

        }

        /*
         * Search produced results, and onMatch action will be triggered.
         */
        if (vertices.hasNext()) {
            return vertices;
        }

        final Map<?,?> onCreateMap = onCreateMap(traverser, mergeMap);

        final Object[] flatArgs = onCreateMap.entrySet().stream()
                .flatMap(e -> Stream.of(e.getKey(), e.getValue())).collect(Collectors.toList()).toArray();

        final Vertex vertex = graph.addVertex(flatArgs);

        // trigger callbacks for eventing - in this case, it's a VertexAddedEvent
        if (this.callbackRegistry != null && !callbackRegistry.getCallbacks().isEmpty()) {
            final EventStrategy eventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class).get();
            final Event.VertexAddedEvent vae = new Event.VertexAddedEvent(eventStrategy.detach(vertex));
            this.callbackRegistry.getCallbacks().forEach(c -> c.accept(vae));
        }

        return IteratorUtils.of(vertex);
    }

    protected Map onCreateMap(final Traverser.Admin<S> traverser, final Map mergeMap) {
        // no onCreateTraversal - use main mergeMap argument
        if (onCreateTraversal == null)
            return mergeMap;

        final Map onCreateMap = materializeMap(traverser, onCreateTraversal);
        // null result from onCreateTraversal - use main mergeMap argument
        if (onCreateMap == null)
            return mergeMap;

        validateMapInput(onCreateMap, false);

        /*
         * Now we need to merge the two maps - onCreate should inherit traits from mergeMap, and it is not allowed to
         * override values for any keys.
         */
        validateNoOverrides(mergeMap, onCreateMap);
        onCreateMap.putAll(mergeMap);

        return onCreateMap;
    }

}
