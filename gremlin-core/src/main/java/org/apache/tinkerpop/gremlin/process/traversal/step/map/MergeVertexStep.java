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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
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
        if (search.containsKey(T.id))
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

        Stream<Vertex> stream = createSearchStream(searchCreate);
        stream = stream.map(v -> {
            // if no onMatch is defined then there is no update - return the vertex unchanged
            if (null == onMatchTraversal) return v;

            // assume good input from GraphTraversal - folks might drop in a T here even though it is immutable
            final Map<String, Object> onMatchMap = TraversalUtil.apply(traverser, onMatchTraversal);
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
                v.property(key, value);
            });

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
            final Map<Object,Object> m = null == onCreateTraversal ? searchCreate : TraversalUtil.apply(traverser, onCreateTraversal);
            final List<Object> keyValues = new ArrayList<>();
            for (Map.Entry<Object, Object> entry : m.entrySet()) {
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
