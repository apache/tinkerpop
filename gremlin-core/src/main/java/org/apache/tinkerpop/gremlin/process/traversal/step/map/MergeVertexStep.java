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
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Implementation for the {@code mergeV()} step covering both the start step version and the one used mid-traversal.
 */
public class MergeVertexStep<S> extends FlatMapStep<S, Vertex> implements Mutating<Event>,
        TraversalOptionParent<Merge, S, Vertex> {

    private final boolean isStart;
    private boolean first = true;
    private Traversal.Admin<S,Map<Object, Object>> searchCreateTraversal;
    private Traversal.Admin<S, Map<Object, Object>> onCreateTraversal = null;
    private Traversal.Admin<S, Map<String, Object>> onMatchTraversal = null;

    protected CallbackRegistry<Event> callbackRegistry;

    public MergeVertexStep(final Traversal.Admin traversal, final boolean isStart) {
        this(traversal, isStart, new IdentityTraversal());
    }

    public MergeVertexStep(final Traversal.Admin traversal, final boolean isStart, final Map<Object, Object> searchCreate) {
        this(traversal, isStart, new ConstantTraversal<>(searchCreate));
    }

    public MergeVertexStep(final Traversal.Admin traversal, final boolean isStart, final Traversal.Admin<S,Map<Object, Object>> searchCreateTraversal) {
        super(traversal);
        this.isStart = isStart;
        this.searchCreateTraversal = integrateChild(searchCreateTraversal);
    }

    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be used to search for vertices.
     * This {@code Map} also will be used as the default data set to be used to create a vertex if the search is not
     * successful.
     */
    public Traversal.Admin<S, Map<Object, Object>> getSearchCreateTraversal() {
        return searchCreateTraversal;
    }

    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be the override to the one provided
     * by the {@link #getSearchCreateTraversal()} for vertex creation events.
     */
    public Traversal.Admin<S, Map<Object, Object>> getOnCreateTraversal() {
        return onCreateTraversal;
    }

    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be used to modify vertices that
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
    public void addChildOption(final Merge token, final Traversal.Admin<S, Vertex> traversalOption) {
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
    protected Traverser.Admin<Vertex> processNextStart() {
        // when it's a start step a traverser needs to be created to kick off the traversal.
        if (isStart && first) {
            first = false;
            final TraverserGenerator generator = this.getTraversal().getTraverserGenerator();
            this.addStart(generator.generate(false, (Step) this, 1L));
        }
        return super.processNextStart();
    }

    /**
     * Use the {@code Map} of search criteria to most efficiently return a {@code Stream<Vertex>} of matching elements.
     * Providers might override this method when extending this step to provide their own optimized mechanisms for
     * matching the list of vertices. This implementation is only optimized for the {@link T#id} so any other usage
     * will simply be in-memory filtering which could be slow.
     */
    protected Stream<Vertex> createSearchStream(final Map<Object,Object> search) {
        final Graph graph = this.getTraversal().getGraph().get();

        Stream<Vertex> stream;
        // prioritize lookup by id
        if (null == search)
            return Stream.empty();
        else if (search.containsKey(T.id))
            stream = IteratorUtils.stream(graph.vertices(search.get(T.id)));
        else
            stream = IteratorUtils.stream(graph.vertices());

        // in-memory filter is not going to be nice here. it will be up to graphs to optimize this step as they do
        // for other Mutation steps
        stream = stream.filter(v -> {
            // try to match on all search criteria skipping T.id as it was handled above
            return search.entrySet().stream().filter(kv -> kv.getKey() != T.id).allMatch(kv -> {
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

    @Override
    protected Iterator<Vertex> flatMap(final Traverser.Admin<S> traverser) {
        final Map<Object,Object> searchCreate = TraversalUtil.apply(traverser, searchCreateTraversal);
        validateMapInput(searchCreate, false);

        Stream<Vertex> stream = createSearchStream(searchCreate);
        stream = stream.map(v -> {
            // if no onMatch is defined then there is no update - return the vertex unchanged
            if (null == onMatchTraversal) return v;

            // assume good input from GraphTraversal - folks might drop in a T here even though it is immutable
            final Map<String, Object> onMatchMap = TraversalUtil.apply(traverser, onMatchTraversal);
            validateMapInput(onMatchMap, true);

            if (onMatchMap != null) {
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
                    final Graph graph = this.getTraversal().getGraph().get();
                    v.property(graph.features().vertex().getCardinality(key), key, value);
                });
            }

            return v;
        });

        // if the stream has something then there is a match (possibly updated) and is returned, otherwise a new
        // vertex is created
        final Iterator<Vertex> vertices = stream.iterator();
        if (vertices.hasNext()) {
            return vertices;
        } else {
            final Vertex vertex;

            // if there is an onCreateTraversal then the search criteria is ignored for the creation as it is provided
            // by way of the traversal which will return the Map
            final boolean useOnCreate = onCreateTraversal != null;
            final Map<Object,Object> onCreateMap = useOnCreate ? TraversalUtil.apply(traverser, onCreateTraversal) : searchCreate;

            // searchCreate should have already been validated so only do it if it is overridden
            if (useOnCreate) validateMapInput(onCreateMap, false);

            // if onCreate is null then it's a do nothing
            final List<Object> keyValues = new ArrayList<>();
            if (onCreateMap != null) {
                for (Map.Entry<Object, Object> entry : onCreateMap.entrySet()) {
                    keyValues.add(entry.getKey());
                    keyValues.add(entry.getValue());
                }
                vertex = this.getTraversal().getGraph().get().addVertex(keyValues.toArray(new Object[keyValues.size()]));

                // trigger callbacks for eventing - in this case, it's a VertexAddedEvent
                if (this.callbackRegistry != null && !callbackRegistry.getCallbacks().isEmpty()) {
                    final EventStrategy eventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class).get();
                    final Event.VertexAddedEvent vae = new Event.VertexAddedEvent(eventStrategy.detach(vertex));
                    this.callbackRegistry.getCallbacks().forEach(c -> c.accept(vae));
                }

                return IteratorUtils.of(vertex);
            } else {
                return Collections.emptyIterator();
            }
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
                return k != T.id && k != T.label && !(k instanceof String);
            }).findFirst().map(e -> {
                throw new IllegalArgumentException(String.format(
                        "mergeV() and option(onCreate) expects keys in Map to be of String, T.id, T.label - check: %s",
                        e.getKey()));
            });
        }

        if (!ignoreTokens) {
            if (m.containsKey(T.id)) {
                if (null == m.get(T.id)) {
                    throw new IllegalArgumentException("Vertex id cannot be null");
                }
            }

            if (m.containsKey(T.label)) {
                final Object l = m.get(T.label);
                if (null == l) {
                    throw new IllegalArgumentException("Vertex label cannot be null");
                }

                if (!(l instanceof String)) {
                    throw new IllegalArgumentException(String.format(
                            "mergeV() expects T.label value to be of String - found: %s",
                            l.getClass().getSimpleName()));
                }
            }
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
    public MergeVertexStep<S> clone() {
        final MergeVertexStep<S> clone = (MergeVertexStep<S>) super.clone();
        clone.searchCreateTraversal = searchCreateTraversal.clone();
        clone.onCreateTraversal = onCreateTraversal != null ? onCreateTraversal.clone() : null;
        clone.onMatchTraversal = onMatchTraversal != null ? onMatchTraversal.clone() : null;
        return clone;
    }
}
