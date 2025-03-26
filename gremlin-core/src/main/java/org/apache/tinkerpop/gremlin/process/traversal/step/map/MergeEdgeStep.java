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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventUtil;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outV;

/**
 * Implementation for the {@code mergeE()} step covering both the start step version and the one used mid-traversal.
 */
public class MergeEdgeStep<S> extends MergeElementStep<S, Edge, Object> {

    private static final Set allowedTokens = new LinkedHashSet(Arrays.asList(T.id, T.label, Direction.IN, Direction.OUT));

    public static void validateMapInput(final Map map, final boolean ignoreTokens) {
        MergeElementStep.validate(map, ignoreTokens, allowedTokens, "mergeE");
    }

    private Traversal.Admin<S, Object> outVTraversal = null;
    private Traversal.Admin<S, Object> inVTraversal = null;

    public MergeEdgeStep(final Traversal.Admin traversal, final boolean isStart) {
        super(traversal, isStart);
    }

    public MergeEdgeStep(final Traversal.Admin traversal, final boolean isStart, final Map merge) {
        super(traversal, isStart, merge);
    }

    public MergeEdgeStep(final Traversal.Admin traversal, final boolean isStart, final GValue<Map> merge) {
        super(traversal, isStart, merge);
    }

    public MergeEdgeStep(final Traversal.Admin traversal, final boolean isStart, final Traversal.Admin<S,Map> mergeTraversal) {
        super(traversal, isStart, mergeTraversal);
    }

    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be used to identify the Direction.OUT
     * vertex during merge.
     */
    public Traversal.Admin<S, Object> getOutVTraversal() {
        return outVTraversal;
    }

    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be used to identify the Direction.IN
     * vertex during merge.
     */
    public Traversal.Admin<S, Object> getInVTraversal() {
        return inVTraversal;
    }

    @Override
    public void addChildOption(final Merge token, final Traversal.Admin<S, Object> traversalOption) {
        if (token == Merge.outV) {
            this.outVTraversal = this.integrateChild(traversalOption);
        } else if (token == Merge.inV) {
            this.inVTraversal = this.integrateChild(traversalOption);
        } else {
            super.addChildOption(token, traversalOption);
        }
    }

    @Override
    public <S, C> List<Traversal.Admin<S, C>> getLocalChildren() {
        final List<Traversal.Admin<S, C>> children = super.getLocalChildren();
        if (outVTraversal != null) children.add((Traversal.Admin<S, C>) outVTraversal);
        if (inVTraversal != null) children.add((Traversal.Admin<S, C>) inVTraversal);
        return children;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (outVTraversal != null)
            result ^= outVTraversal.hashCode();
        if (inVTraversal != null)
            result ^= inVTraversal.hashCode();
        return result;
    }

    @Override
    public void reset() {
        super.reset();
        if (outVTraversal != null) outVTraversal.reset();
        if (inVTraversal != null) inVTraversal.reset();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, mergeTraversal, onCreateTraversal, onMatchTraversal, outVTraversal, inVTraversal);
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(outVTraversal);
        this.integrateChild(inVTraversal);
    }

    @Override
    public MergeEdgeStep<S> clone() {
        final MergeEdgeStep<S> clone = (MergeEdgeStep<S>) super.clone();
        clone.outVTraversal = outVTraversal != null ? outVTraversal.clone() : null;
        clone.inVTraversal = inVTraversal != null ? inVTraversal.clone() : null;
        return clone;
    }

    @Override
    protected Set getAllowedTokens() {
        return allowedTokens;
    }

    /**
     * Translate the Map into search criteria. Default implementation is to translate the Map into a g.E() or
     * g.V().out/inE() traversal. Graph providers will presumably optimize this traversal to use whatever indices are
     * present and appropriate for efficiency.
     *
     * Callers are responsible for closing this iterator when finished.
     */
    protected CloseableIterator<Edge> searchEdges(final Map search) {
        if (search == null)
            return CloseableIterator.empty();

        final Graph graph = getGraph();

        final Object edgeId = search.get(T.id);
        final String edgeLabel = (String) search.get(T.label);
        final Object fromId = search.get(Direction.OUT);
        final Object toId = search.get(Direction.IN);

        GraphTraversal t;
        if (edgeId != null) {

            // g.E(eid).hasLabel(label).where(outV().hasId(fromId)).where(inV().hasId(toId));
            t = graph.traversal().E(edgeId);
            if (edgeLabel != null)
                t = t.hasLabel(edgeLabel);
            if (fromId != null)
                t = t.where(outV().hasId(fromId));
            if (toId != null)
                t = t.where(inV().hasId(toId));

        } else if (fromId != null) {

            // g.V(fromId).outE(label).where(inV().hasId(toId));
            t = graph.traversal().V(fromId);
            if (edgeLabel != null)
                t = t.outE(edgeLabel);
            else
                t = t.outE();
            if (toId != null)
                t = t.where(inV().hasId(toId));

        } else if (toId != null) {

            // g.V(toId).inE(edgeLabel);
            t = graph.traversal().V(toId);
            if (edgeLabel != null)
                t = t.inE(edgeLabel);
            else
                t = t.inE();

        } else {

            // g.E().hasLabel(label)
            t = graph.traversal().E();
            if (edgeLabel != null)
                t = t.hasLabel(edgeLabel);

        }

        // add property constraints
        for (final Map.Entry e : ((Map<?,?>) search).entrySet()) {
            final Object k = e.getKey();
            if (!(k instanceof String)) continue;
            t = t.has((String) k, e.getValue());
        }

        // this should auto-close the underlying traversal
        return CloseableIterator.of(t);
    }

    protected Map<?,?> resolveVertices(final Map map, final Traverser.Admin<S> traverser) {
        resolveVertex(Merge.outV, Direction.OUT, map, traverser, outVTraversal);
        resolveVertex(Merge.inV, Direction.IN, map, traverser, inVTraversal);
        return map;
    }

    protected void resolveVertex(final Merge token, final Direction direction, final Map map,
            final Traverser.Admin<S> traverser, final Traversal.Admin<S, Object> traversal) {
        // no Direction specified in the map, nothing to resolve
        if (!map.containsKey(direction))
            return;

        final Object value = map.get(direction);
        if (Objects.equals(token, value)) {
            if (traversal == null) {
                throw new IllegalArgumentException(String.format(
                        "option(%s) must be specified if it is used for %s", token, direction));
            }
            final Vertex vertex = resolveVertex(traverser, traversal);
            if (vertex == null)
                throw new IllegalArgumentException(String.format(
                        "Could not resolve vertex for option(%s)", token));
            map.put(direction, vertex.id());
        } else if (value instanceof Vertex) {
            // flatten Vertex down to its id
            map.put(direction, ((Vertex) value).id());
        }
    }

    @Override
    protected Iterator<Edge> flatMap(final Traverser.Admin<S> traverser) {
        final Map unresolvedMergeMap = materializeMap(traverser, mergeTraversal);
        validateMapInput(unresolvedMergeMap, false);

        /*
         * Create a copy of the unresolved map and attempt to resolve any Vertex references.
         */
        final Map mergeMap = resolveVertices(new LinkedHashMap<>(unresolvedMergeMap), traverser);

        Iterator<Edge> edges = searchEdges(mergeMap);

        if (onMatchTraversal != null) {
            if (onMatchTraversal instanceof ConstantTraversal) {
                final Map matchMap = onMatchTraversal.next();
                validateMapInput(matchMap, true);
            }

            edges = IteratorUtils.peek(edges, e -> {

                // override current traverser with the matched Edge so that the option() traversal can operate
                // on it properly. prior to 4.x this only worked for start steps, but now it works consistently
                // with mid-traversal usage. this breaks past behavior like g.inject(Map).mergeE() where you
                // could operate on the Map directly with the child traversal. from 4.x onward you will have to do
                // something like g.inject(Map).as('a').mergeE().option(onMatch, select('a'))
                traverser.set((S) e);

                // assume good input from GraphTraversal - folks might drop in a T here even though it is immutable
                final Map<String, ?> onMatchMap = materializeMap(traverser, onMatchTraversal);
                validateMapInput(onMatchMap, true);

                onMatchMap.forEach((key, value) -> {
                    // trigger callbacks for eventing - in this case, it's a EdgePropertyChangedEvent. if there's no
                    // registry/callbacks then just set the property
                    EventUtil.registerEdgePropertyChange(callbackRegistry, getTraversal(), e, key, value);
                    e.property(key, value);
                });

            });

        }

        /*
         * Search produced results, and onMatch action will be triggered.
         */
        if (edges.hasNext()) {
            return edges;
        }

        // make sure we close the search traversal
        CloseableIterator.closeIterator(edges);

        /*
         * This onCreateMap will inherit from the main merge argument - a union of merge and onCreate with no overrides
         * allowed.
         */
        final Map onCreateMap = onCreateMap(traverser, unresolvedMergeMap, mergeMap);

        // check for from/to vertices, which must be specified for the create action
        if (!onCreateMap.containsKey(Direction.OUT))
            throw new IllegalArgumentException("Out Vertex not specified in onCreate - edge cannot be created");
        if (!onCreateMap.containsKey(Direction.IN))
            throw new IllegalArgumentException("In Vertex not specified in onCreate - edge cannot be created");

        final Vertex fromV = resolveVertex(onCreateMap.get(Direction.OUT));
        final Vertex toV = resolveVertex(onCreateMap.get(Direction.IN));
        final String label = (String) onCreateMap.getOrDefault(T.label, Edge.DEFAULT_LABEL);

        final List<Object> properties = new ArrayList<>();

        // add property constraints
        for (final Map.Entry e : ((Map<?,?>) onCreateMap).entrySet()) {
            final Object k = e.getKey();
            if (k.equals(Direction.OUT) || k.equals(Direction.IN) || k.equals(T.label)) continue;
            properties.add(k);
            properties.add(e.getValue());
        }

        final Edge edge = fromV.addEdge(label, toV, properties.toArray());

        // trigger callbacks for eventing - in this case, it's a VertexAddedEvent
        EventUtil.registerEdgeCreationWithGenericEventRegistry(callbackRegistry, getTraversal(), edge);

        return IteratorUtils.of(edge);
    }

    protected Map onCreateMap(final Traverser.Admin<S> traverser, final Map unresolvedMergeMap, final Map mergeMap) {
        // no onCreateTraversal - use main mergeMap argument
        if (onCreateTraversal == null)
            return mergeMap;

        final Map onCreateMap = materializeMap(traverser, onCreateTraversal);
        // null result from onCreateTraversal - use main mergeMap argument
        if (onCreateMap == null || onCreateMap.size() == 0)
            return mergeMap;
        validateMapInput(onCreateMap, false);

        /*
         * Now we need to merge the two maps - onCreate should inherit traits from mergeMap, and it is not allowed to
         * override values for any keys.
         */

        /*
         * We use the unresolved version here in case onCreateMap uses Merge tokens or Vertex objects for its values.
         */
        validateNoOverrides(unresolvedMergeMap, onCreateMap);

        /*
         * Use the resolved version here so that onCreateMap picks up fully resolved vertex arguments from the main
         * merge argument and so we don't re-resolve them below.
         */
        final Map<Object, Object> combinedMap = new HashMap<>(onCreateMap.size() + mergeMap.size());
        combinedMap.putAll(onCreateMap);
        combinedMap.putAll(mergeMap);

        /*
         * Do any final vertex resolution, for example if Merge tokens were used in option(onCreate) but not in the main
         * merge argument.
         */
        resolveVertices(combinedMap, traverser);

        return combinedMap;
    }

    /*
     * Merge.outV/inV traversals can either provide a Map (which we then use to search for a vertex) or it can provide a
     * Vertex directly, e.g. select from a labeled mergeV.
     */
    protected Vertex resolveVertex(final Traverser.Admin<S> traverser, final Traversal.Admin<S, Object> traversal) {
        final Object o = TraversalUtil.apply(traverser, traversal);
        if (o instanceof Vertex)
            return (Vertex) o;
        else if (o instanceof Map) {
            final Map search = (Map) o;
            final Vertex v = IteratorUtils.findFirst(searchVertices(search)).get();
            return tryAttachVertex(v);
        }
        throw new IllegalArgumentException(
                String.format("Vertex does not exist for mergeE: %s", o));
    }

    /*
     * Resolve the argument for Direction.IN/OUT into a proper Vertex.
     */
    protected Vertex resolveVertex(final Object arg) {
        // arg might already be a Vertex
        if (arg instanceof Vertex)
            return tryAttachVertex((Vertex) arg);

        // otherwise use the arg as a vertex id
        try (CloseableIterator<Vertex> it = CloseableIterator.of(getGraph().vertices(arg))) {
            if (!it.hasNext())
                throw new IllegalArgumentException(
                        String.format("Vertex does not exist for mergeE: %s", arg));
            return it.next();
        }
    }

    /**
     * Tries to attach a {@link Vertex} to its host {@link Graph} of the traversal. If the {@link Vertex} cannot be
     * found then an {@code IllegalArgumentException} is expected.
     */
    protected Vertex tryAttachVertex(final Vertex v) {
        if (v instanceof Attachable) {
            try {
                return ((Attachable<Vertex>) v).attach(Attachable.Method.get(getGraph()));
            } catch (IllegalStateException ise) {
                throw new IllegalArgumentException(
                        String.format("Vertex does not exist for mergeE: %s", v));
            }
        } else {
            return v;
        }
    }

}
