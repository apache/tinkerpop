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
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Implementation for the {@code mergeE()} step covering both the start step version and the one used mid-traversal.
 */
public class MergeEdgeStep<S> extends FlatMapStep<S, Edge> implements Mutating<Event>,
        TraversalOptionParent<Merge, S, Edge> {

    public static final Vertex PLACEHOLDER_VERTEX = new ReferenceVertex(Graph.Hidden.hide(MergeEdgeStep.class.getName()));
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

    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be used to search for edges.
     * This {@code Map} also will be used as the default data set to be used to create a edge if the search is not
     * successful.
     */
    public Traversal.Admin<S, Map<Object, Object>> getSearchCreateTraversal() {
        return searchCreateTraversal;
    }

    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be the override to the one provided
     * by the {@link #getSearchCreateTraversal()} for edge creation events.
     */
    public Traversal.Admin<S, Map<Object, Object>> getOnCreateTraversal() {
        return onCreateTraversal;
    }

    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be used to modify edges that
     * match the search criteria of {@link #getSearchCreateTraversal()}.
     */
    public Traversal.Admin<S, Map<String, Object>> getOnMatchTraversal() {
        return onMatchTraversal;
    }

    /**
     * Determines if this is a start step.
     */
    public boolean isStart() {
        return isStart;
    }

    /**
     * Determine if this is the first pass through {@link #processNextStart()}.
     */
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
        // this is a Mutating step but property() should not be folded into this step.  The main issue here is that
        // this method won't know what step called it - property() or with() or something else so it can't make the
        // choice easily to throw an exception, write the keys/values to parameters, etc. It really is up to the
        // caller to make sure it is handled properly at this point. this may best be left as a do-nothing method for
        // now.
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
            this.addStart(generator.generate(PLACEHOLDER_VERTEX, (Step) this, 1L));
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
        if (null == search) {
            return Stream.empty();
        } else if (search.containsKey(T.id)) {
            stream = IteratorUtils.stream(graph.edges(search.get(T.id)));
            directionUsedInLookup = Optional.empty();
        } else if (search.containsKey(Direction.BOTH)) {
            // filter self-edges with distinct()
            stream = IteratorUtils.stream(graph.vertices(search.get(Direction.BOTH))).flatMap(v -> IteratorUtils.stream(v.edges(Direction.BOTH))).distinct();
            directionUsedInLookup = Optional.of(Direction.BOTH);
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
                            final Iterator<Vertex> dfound = e.vertices(direction);
                            final boolean matched = found.hasNext() && dfound.next().equals(found.next());
                            CloseableIterator.closeIterator(found);
                            CloseableIterator.closeIterator(dfound);
                            return matched;
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
        final Map<Object,Object> searchCreate = TraversalUtil.apply(traverser, searchCreateTraversal);

        validateMapInput(searchCreate, false);

        final Vertex outV = resolveVertex(traverser, searchCreate, Direction.OUT);
        final Vertex inV = resolveVertex(traverser, searchCreate, Direction.IN);

        // need to copy searchCreate so that each traverser gets fresh search criteria if we use the traverser value
        final Map<Object,Object> searchCreateCopy = null == searchCreate ? null : new HashMap<>();
        if (searchCreateCopy != null) {
            searchCreateCopy.putAll(searchCreate);

            // out/in not specified in searchCreate so try to use the traverser. BOTH is not an accepted user input
            // but helps with the search stream as it allows in/out to both be in the search stream. in other words,
            // g.V().mergeE([label:'knows']) will end up traversing BOTH "knows" edges for each vertex
            if (!searchCreateCopy.containsKey(Direction.OUT) && !searchCreateCopy.containsKey(Direction.IN) &&
                    outV == inV && inV != PLACEHOLDER_VERTEX) {
                searchCreateCopy.put(Direction.BOTH, outV);
            }
        }

        Stream<Edge> stream = createSearchStream(searchCreateCopy);
        stream = stream.map(e -> {
            // if no onMatch is defined then there is no update - return the edge unchanged
            if (null == onMatchTraversal) return e;

            // if this was a start step the traverser is initialized with placeholder edge, so override that with
            // the matched Edge so that the option() traversal can operate on it properly
            if (isStart) traverser.set((S) e);

            // assume good input from GraphTraversal - folks might drop in a T here even though it is immutable
            final Map<String, Object> onMatchMap = TraversalUtil.apply(traverser, onMatchTraversal);
            validateMapInput(onMatchMap, true);

            if (onMatchMap != null) {
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
            }

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
            final boolean useOnCreate = onCreateTraversal != null;
            final Map<Object,Object> onCreateMap = useOnCreate ? TraversalUtil.apply(traverser, onCreateTraversal) : searchCreateCopy;

            // searchCreate should have already been validated so only do it if it is overridden
            if (useOnCreate) validateMapInput(onCreateMap, false);

            if (onCreateMap != null) {
                // check if from/to were already determined by traverser/searchMatch, and if not, then at least ensure that
                // the from/to is set in onCreateMap
                if (outV == PLACEHOLDER_VERTEX && !onCreateMap.containsKey(Direction.OUT))
                    throw new IllegalArgumentException("Out Vertex not specified - edge cannot be created");
                if (inV == PLACEHOLDER_VERTEX && !onCreateMap.containsKey(Direction.IN))
                    throw new IllegalArgumentException("In Vertex not specified - edge cannot be created");

                final List<Object> keyValues = new ArrayList<>();
                String label = Edge.DEFAULT_LABEL;

                // assume the to/from vertices from traverser/searchMatch are what we want, but then
                // consider the override dropping in from onCreate
                Vertex fromV = outV;
                Vertex toV = inV;

                for (Map.Entry<Object, Object> entry : onCreateMap.entrySet()) {
                    if (entry.getKey() instanceof Direction) {
                        // only override if onCreate was specified otherwise stick to however traverser/searchMatch
                        // was resolved
                        if (useOnCreate && entry.getKey().equals(Direction.IN)) {
                            final Object o = searchCreateCopy.getOrDefault(Direction.IN, entry.getValue());
                            toV = tryAttachVertex(o instanceof Vertex ? (Vertex) o : new ReferenceVertex(o));
                        } else if (useOnCreate && entry.getKey().equals(Direction.OUT)) {
                            final Object o = searchCreateCopy.getOrDefault(Direction.OUT, entry.getValue());
                            fromV = tryAttachVertex(o instanceof Vertex ? (Vertex) o : new ReferenceVertex(o));
                        }
                    } else if (entry.getKey().equals(T.label)) {
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
            } else {
                return Collections.emptyIterator();
            }
        }
    }

    /**
     * Little helper method that will resolve {@link Direction} map keys to a {@link Vertex} which is the currency
     * of this step. Since this step can accept a {@link Vertex} as the traverser it uses that as a default value
     * in the case where {@link Direction} is not specified in the {@code Map}. As a result the {@code Map} value
     * overrides the traverser. Note that if this is a start step then the traverser will contain a
     * {@link #PLACEHOLDER_VERTEX} which is basically just a dummy to use as a marker where it will be assumed a
     * {@code Map} argument to the step will have the necessary {@link Vertex} to allow the step to do its work. If
     * the {@link Direction} contains something other than a {@link Vertex} it will become the {@link T#id} to a
     * fresh {@link ReferenceVertex}.
     */
    protected Vertex resolveVertex(final Traverser.Admin<S> traverser, final Map<Object, Object> searchCreate,
                                   final Direction direction) {
        final Vertex traverserVertex = traverser.get() instanceof Vertex ? (Vertex) traverser.get() : PLACEHOLDER_VERTEX;
        final Object o = searchCreate != null ? searchCreate.getOrDefault(direction, traverserVertex) : traverserVertex;
        final Vertex v = o instanceof Vertex ? (Vertex) o : new ReferenceVertex(o);
        if (v != PLACEHOLDER_VERTEX && v instanceof Attachable) {
            return tryAttachVertex(v);
        } else {
            return v;
        }
    }

    /**
     * Tries to attach a {@link Vertex} to its host {@link Graph} of the traversal. If the {@link Vertex} cannot be
     * found then an {@code IllegalArgumentException} is expected.
     */
    protected Vertex tryAttachVertex(final Vertex maybeAttachable) {
        if (maybeAttachable instanceof Attachable) {
            try {
                return ((Attachable<Vertex>) maybeAttachable).attach(Attachable.Method.get(this.getTraversal().getGraph().orElse(EmptyGraph.instance())));
            } catch (IllegalStateException ise) {
                throw new IllegalArgumentException(String.format("%s could not be found and edge could not be created", maybeAttachable));
            }
        } else {
            return maybeAttachable;
        }
    }

    /**
     * Validates input to any {@code Map} arguments to this step. For {@link Merge#onMatch} updates cannot be applied
     * to immutable parts of an {@link Edge} (id, label, incident vertices) so those can be ignored in the validation.
     */
    public static void validateMapInput(final Map<?,Object> m, final boolean ignoreTokens) {
        if (null == m) return;
        if (ignoreTokens) {
            m.entrySet().stream().filter(e -> {
                final Object k = e.getKey();
                return !(k instanceof String);
            }).findFirst().map(e -> {
                throw new IllegalArgumentException(String.format(
                        "option(onMatch) expects keys in Map to be of String - check: %s",
                        e.getKey()));
            });
        } else {
            m.entrySet().stream().filter(e -> {
                final Object k = e.getKey();
                return k != T.id && k != T.label && k != Direction.OUT && k != Direction.IN && !(k instanceof String);
            }).findFirst().map(e -> {
                throw new IllegalArgumentException(String.format(
                        "mergeE() and option(onCreate) expects keys in Map to be of String, T.id, T.label, or any Direction except BOTH - check: %s",
                        e.getKey()));
            });
        }

        if (!ignoreTokens) {
            m.entrySet().stream().filter(e -> e.getKey() == T.label && !(e.getValue() instanceof String)).findFirst().map(e -> {
                throw new IllegalArgumentException(String.format(
                        "mergeE() expects T.label value to be of String - found: %s",
                        e.getValue().getClass().getSimpleName()));
            });
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
