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
import org.apache.tinkerpop.gremlin.process.traversal.lambda.CardinalityValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import static java.util.stream.Collectors.toList;

/**
 * Implementation for the {@code mergeV()} step covering both the start step version and the one used mid-traversal.
 */
public class MergeVertexStep<S> extends MergeElementStep<S, Vertex, Map> {

    private static final Set allowedTokens = new LinkedHashSet(Arrays.asList(T.id, T.label));

    public static void validateMapInput(final Map map, final boolean ignoreTokens) {
        MergeElementStep.validate(map, ignoreTokens, allowedTokens, "mergeV");
    }


    public MergeVertexStep(final Traversal.Admin traversal, final boolean isStart) {
        super(traversal, isStart);
    }

    public MergeVertexStep(final Traversal.Admin traversal, final boolean isStart, final Map merge) {
        super(traversal, isStart, merge);
    }

    public MergeVertexStep(final Traversal.Admin traversal, final boolean isStart, final GValue<Map> merge) {
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
            if (onMatchTraversal instanceof ConstantTraversal) {
                final Map matchMap = onMatchTraversal.next();
                validateMapInput(matchMap, true);
            }

            // attach the onMatch properties
            vertices = IteratorUtils.peek(vertices, v -> {

                // override current traverser with the matched Vertex so that the option() traversal can operate
                // on it properly. prior to 4.x this only worked for start steps, but now it works consistently
                // with mid-traversal usage. this breaks past behavior like g.inject(Map).mergeV() where you
                // could operate on the Map directly with the child traversal. from 4.x onward you will have to do
                // something like g.inject(Map).as('a').mergeV().option(onMatch, select('a'))
                traverser.set((S) v);

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
                    EventUtil.registerVertexPropertyChange(callbackRegistry, getTraversal(), v, key, val);

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

        // extract the key/value pairs from the map and flatten them into an array but exclude any that have a
        // CardinalityValueTraversal as the value. you have to ignore those in a call to addVertex because that would
        // make it so that the Graph had to know how to deal with the CardinalityValueTraversal which it doesn't. this
        // allows this feature to work out of the box.
        final Object[] flatArgsWithoutExplicitCardinality = onCreateMap.entrySet().stream().
                filter(e -> !(e.getValue() instanceof CardinalityValueTraversal)).
                flatMap(e -> Stream.of(e.getKey(), e.getValue())).collect(toList()).toArray();

        final Vertex vertex = graph.addVertex(flatArgsWithoutExplicitCardinality);

        // deal with values that have the cardinality explicitly set which should only occur on string keys
        onCreateMap.entrySet().stream().
                filter(e -> e.getKey() instanceof String && e.getValue() instanceof CardinalityValueTraversal).
                forEach(e -> {
                    final CardinalityValueTraversal cardinalityValueTraversal = (CardinalityValueTraversal) e.getValue();
                    vertex.property(cardinalityValueTraversal.getCardinality(), (String) e.getKey(), cardinalityValueTraversal.getValue());
                });

        // trigger callbacks for eventing - in this case, it's a VertexAddedEvent
        EventUtil.registerVertexCreationWithGenericEventRegistry(callbackRegistry, getTraversal(), vertex);

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
