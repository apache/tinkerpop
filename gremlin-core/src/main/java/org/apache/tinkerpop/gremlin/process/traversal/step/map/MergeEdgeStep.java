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

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Implementation for the {@code mergeE()} step covering both the start step version and the one used mid-traversal.
 */
public class MergeEdgeStep<S> extends FlatMapStep<S, Edge> implements Mutating<Event>,
        TraversalOptionParent<Merge, S, Edge> {

    private final boolean isStart;
    private boolean first = true;
    private Traversal.Admin<S,Map<Object, Object>> searchCreateTraversal;
    private Traversal.Admin<S, Map<Object, Object>> onCreateTraversal = null;
    private Traversal.Admin<S, Map<String, Object>> onMatchTraversal = null;

    protected CallbackRegistry<Event> callbackRegistry;

    public MergeEdgeStep(final Traversal.Admin traversal, final boolean isStart) {
        this(traversal, isStart, new IdentityTraversal<>());
    }

    public MergeEdgeStep(final Traversal.Admin traversal, final boolean isStart, final Map<Object, Object> searchCreate) {
        this(traversal, isStart, new ConstantTraversal<>(searchCreate));
    }

    public MergeEdgeStep(final Traversal.Admin traversal, final boolean isStart, final Traversal.Admin<?,Map<Object, Object>> searchCreateTraversal) {
        super(traversal);
        this.isStart = isStart;
        this.searchCreateTraversal = integrateChild(searchCreateTraversal);
    }

    public Traversal.Admin<S, Map<Object, Object>> getSearchCreateTraversal() {
        return searchCreateTraversal;
    }

    public Traversal.Admin<S, Map<Object, Object>> getOnCreateTraversal() {
        return onCreateTraversal;
    }

    public Traversal.Admin<S, Map<String, Object>> getOnMatchTraversal() {
        return onMatchTraversal;
    }

    /**
     * Determines if this is a start step.
     */
    public boolean isStart() {
        return isStart;
    }

    public boolean isFirst() {
        return first;
    }

    public CallbackRegistry<Event> getCallbackRegistry() {
        return callbackRegistry;
    }

    @Override
    public void addChildOption(final Merge token, final Traversal.Admin<S, Edge> traversalOption) {
        if (token == Merge.onCreate) {
            this.onCreateTraversal = this.integrateChild(traversalOption);
        } else if (token == Merge.onMatch) {
            this.onMatchTraversal = this.integrateChild(traversalOption);
        } else {
            throw new UnsupportedOperationException(String.format("Option %s for Merge is not supported", token.name()));
        }
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        final List<Traversal.Admin<S, E>> children = new ArrayList<>();
        if (searchCreateTraversal != null) children.add((Traversal.Admin<S, E>) searchCreateTraversal);
        if (onMatchTraversal != null) children.add((Traversal.Admin<S, E>) onMatchTraversal);
        if (onCreateTraversal != null) children.add((Traversal.Admin<S, E>) onCreateTraversal);
        return children;
    }

    @Override
    public void configure(final Object... keyValues) {
        // this is a Mutating step but property() should not be folded into this step. this exception should not
        // end up visible to users really
    }

    @Override
    public Parameters getParameters() {
        // merge doesn't take fold ups of property() calls. those need to get treated as regular old PropertyStep
        // instances. not sure if this should support with() though.....none of the other Mutating steps do.
        return null;
    }

    @Override
    protected Traverser.Admin<Edge> processNextStart() {
        // when it's a start step a traverser needs to be created to kick off the traversal.
        if (isStart && first) {
            first = false;
            final TraverserGenerator generator = this.getTraversal().getTraverserGenerator();
            this.addStart(generator.generate(false, (Step) this, 1L));
        }
        return super.processNextStart();
    }

    /**
     * Use the {@code Map} of search criteria to most efficiently return a {@code Stream<Edge>} of matching elements.
     * Providers might override this method when extending this step to provide their own optimized mechanisms for
     * matching the list of edges. This implementation is only optimized for the {@link T#id} so any other usage
     * will simply be in-memory filtering which could be slow.
     */
    protected Stream<Edge> createSearchStream(final Map<Object,Object> search) {
        final Graph graph = this.getTraversal().getGraph().get();

        final Optional<Direction> directionUsedInLookup;
        Stream<Edge> stream;
        // prioritize lookup by id, then use vertices as starting point if possible
        if (search.containsKey(T.id)) {
            stream = IteratorUtils.stream(graph.edges(search.get(T.id)));
            directionUsedInLookup = Optional.empty();
        } else if (search.containsKey(Direction.OUT)) {
            stream = IteratorUtils.stream(graph.vertices(search.get(Direction.OUT))).flatMap(v -> IteratorUtils.stream(v.edges(Direction.OUT)));
            directionUsedInLookup = Optional.of(Direction.OUT);
        } else if (search.containsKey(Direction.IN)) {
            stream = IteratorUtils.stream(graph.vertices(search.get(Direction.IN))).flatMap(v -> IteratorUtils.stream(v.edges(Direction.IN)));
            directionUsedInLookup = Optional.of(Direction.IN);
        } else {
            stream = IteratorUtils.stream(graph.edges());
            directionUsedInLookup = Optional.empty();
        }

        // in-memory filter is not going to be nice here. it will be up to graphs to optimize this step as they do
        // for other Mutation steps
        stream = stream.filter(e -> {
            // try to match on all search criteria skipping T.id as it was handled above
            return search.entrySet().stream().filter(
                    kv -> kv.getKey() != T.id && !(directionUsedInLookup.isPresent() && kv.getKey() == directionUsedInLookup.get())).
                    allMatch(kv -> {
                        if (kv.getKey() == T.label) {
                            return e.label().equals(kv.getValue());
                        } else if (kv.getKey() instanceof Direction) {
                            final Direction direction = (Direction) kv.getKey();

                            // try to take advantage of string id conversions of the graph by doing a lookup rather
                            // than direct compare on id
                            final Iterator<Vertex> found = graph.vertices(kv.getValue());
                            return found.hasNext() && e.vertices(direction).next().equals(found.next());
                        } else {
                            final Property<Object> vp = e.property(kv.getKey().toString());
                            return vp.isPresent() && kv.getValue().equals(vp.value());
                        }
                    });
        });

        return stream;
    }

    @Override
    protected Iterator<Edge> flatMap(final Traverser.Admin<S> traverser) {
        final S possibleVertex = traverser.get();
        final Map<Object,Object> searchCreate = TraversalUtil.apply(traverser, searchCreateTraversal);

        Vertex outV = (Vertex) searchCreate.getOrDefault(Direction.OUT, possibleVertex);
        Vertex inV = (Vertex) searchCreate.getOrDefault(Direction.IN, possibleVertex);

        if (inV instanceof Attachable)
            inV = ((Attachable<Vertex>) inV)
                    .attach(Attachable.Method.getOrCreate(this.getTraversal().getGraph().orElse(EmptyGraph.instance())));
        if (outV instanceof Attachable)
            outV = ((Attachable<Vertex>) outV)
                    .attach(Attachable.Method.getOrCreate(this.getTraversal().getGraph().orElse(EmptyGraph.instance())));

        final Vertex fromV = outV;
        final Vertex toV = inV;

        Stream<Edge> stream = createSearchStream(searchCreate);
        stream = stream.map(e -> {
            // if no onMatch is defined then there is no update - return the edge unchanged
            if (null == onMatchTraversal) return e;

            // assume good input from GraphTraversal - folks might drop in a T here even though it is immutable
            final Map<String, Object> onMatchMap = TraversalUtil.apply(traverser, onMatchTraversal);
            onMatchMap.forEach((key, value) -> {
                // trigger callbacks for eventing - in this case, it's a EdgePropertyChangedEvent. if there's no
                // registry/callbacks then just set the property
                if (this.callbackRegistry != null && !callbackRegistry.getCallbacks().isEmpty()) {
                    final EventStrategy eventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class).get();
                    final Property<?> p = e.property(key);
                    final Property<Object> oldValue = p.isPresent() ? eventStrategy.detach(e.property(key)) : null;
                    final Event.EdgePropertyChangedEvent vpce = new Event.EdgePropertyChangedEvent(eventStrategy.detach(e), oldValue, value);
                    this.callbackRegistry.getCallbacks().forEach(c -> c.accept(vpce));
                }
                e.property(key, value);
            });

            return e;
        });

        // if the stream has something then there is a match (possibly updated) and is returned, otherwise a new
        // edge is created
        final Iterator<Edge> edges = stream.iterator();
        if (edges.hasNext()) {
            return edges;
        } else {
            final Edge edge;

            // if there is an onCreateTraversal then the search criteria is ignored for the creation as it is provided
            // by way of the traversal which will return the Map
            final Map<Object,Object> m = null == onCreateTraversal ? searchCreate : TraversalUtil.apply(traverser, onCreateTraversal);
            final List<Object> keyValues = new ArrayList<>();
            String label = Edge.DEFAULT_LABEL;
            for (Map.Entry<Object, Object> entry : m.entrySet()) {
                // skip Direction keys as they are already handled in getting fromV/toV
                if (entry.getKey() instanceof Direction) continue;

                if (entry.getKey().equals(T.label)) {
                    label = (String) entry.getValue();
                } else {
                    keyValues.add(entry.getKey());
                    keyValues.add(entry.getValue());
                }
            }

            edge = fromV.addEdge(label, toV, keyValues.toArray(new Object[keyValues.size()]));

            // trigger callbacks for eventing - in this case, it's a VertexAddedEvent
            if (this.callbackRegistry != null && !callbackRegistry.getCallbacks().isEmpty()) {
                final EventStrategy eventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class).get();
                final Event.EdgeAddedEvent vae = new Event.EdgeAddedEvent(eventStrategy.detach(edge));
                this.callbackRegistry.getCallbacks().forEach(c -> c.accept(vae));
            }

            return IteratorUtils.of(edge);
        }
    }

    @Override
    public CallbackRegistry<Event> getMutatingCallbackRegistry() {
        if (null == callbackRegistry) callbackRegistry = new ListCallbackRegistry<>();
        return callbackRegistry;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (searchCreateTraversal != null)
            result ^= searchCreateTraversal.hashCode();
        if (onCreateTraversal != null)
            result ^= onCreateTraversal.hashCode();
        if (onMatchTraversal != null)
            result ^= onMatchTraversal.hashCode();
        return result;
    }

    @Override
    public void reset() {
        super.reset();
        first = true;
        searchCreateTraversal.reset();
        if (onCreateTraversal != null) onCreateTraversal.reset();
        if (onMatchTraversal != null) onMatchTraversal.reset();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, searchCreateTraversal, onCreateTraversal, onMatchTraversal);
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(searchCreateTraversal);
        this.integrateChild(onCreateTraversal);
        this.integrateChild(onMatchTraversal);
    }

    @Override
    public MergeEdgeStep<S> clone() {
        final MergeEdgeStep<S> clone = (MergeEdgeStep<S>) super.clone();
        clone.searchCreateTraversal = searchCreateTraversal.clone();
        clone.onCreateTraversal = onCreateTraversal != null ? onCreateTraversal.clone() : null;
        clone.onMatchTraversal = onMatchTraversal != null ? onMatchTraversal.clone() : null;
        return clone;
    }
}
