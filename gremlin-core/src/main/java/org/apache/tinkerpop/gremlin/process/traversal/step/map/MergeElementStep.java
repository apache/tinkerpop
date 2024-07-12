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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Deleting;
import org.apache.tinkerpop.gremlin.process.traversal.step.GType;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.Writing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * Abstract base class for the {@code mergeV/E()} implementations.
 */
public abstract class MergeElementStep<S, E, C> extends FlatMapStep<S, E>
        implements Writing<Event>, Deleting<Event>, TraversalOptionParent<Merge, S, C> {

    protected final boolean isStart;
    protected boolean first = true;
    protected Traversal.Admin<S, Map> mergeTraversal;
    protected Traversal.Admin<S, Map> onCreateTraversal = null;
    protected Traversal.Admin<S, Map<String, ?>> onMatchTraversal = null;

    protected CallbackRegistry<Event> callbackRegistry;

    private Parameters parameters = new Parameters();

    protected boolean usesPartitionStrategy;

    public MergeElementStep(final Traversal.Admin traversal, final boolean isStart) {
        this(traversal, isStart, new IdentityTraversal<>());
    }

    public MergeElementStep(final Traversal.Admin traversal, final boolean isStart, final Map mergeMap) {
        this(traversal, isStart, new ConstantTraversal<>(mergeMap));
        validate(mergeMap, false);
    }

    public MergeElementStep(final Traversal.Admin traversal, final boolean isStart, final GValue<Map> mergeMap) {
        this(traversal, isStart, new ConstantTraversal<>(mergeMap));
        validate(mergeMap.get(), false);
    }

    public MergeElementStep(final Traversal.Admin traversal, final boolean isStart,
                            final Traversal.Admin mergeTraversal) {
        super(traversal);
        this.isStart = isStart;
        this.mergeTraversal = integrateChild(mergeTraversal);

        // determines if this step uses PartitionStrategy. it's not great that merge needs to know about a particular
        // strategy but if it doesn't then it can't determine if Parameters are being used properly or not. to not have
        // this check seems to invite problems. in some sense, this is not the first time steps have had to know more
        // about strategies than is probably preferred - EventStrategy comes to mind
        this.usesPartitionStrategy = TraversalHelper.getRootTraversal(traversal).
                getStrategies().getStrategy(PartitionStrategy.class).isPresent();
    }

    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be used to search for elements.
     * This {@code Map} also will be used as the default data set to be used to create the element if the search is not
     * successful.
     */
    public Traversal.Admin<S, Map> getMergeTraversal() {
        return mergeTraversal;
    }

    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be used to create elements that
     * do not match the search criteria of {@link #getMergeTraversal()}.
     */
    public Traversal.Admin<S, Map> getOnCreateTraversal() {
        return onCreateTraversal;
    }

    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be used to modify elements that
     * match the search criteria of {@link #getMergeTraversal()}.
     */
    public Traversal.Admin<S, Map<String, ?>> getOnMatchTraversal() {
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
    public void addChildOption(final Merge token, final Traversal.Admin<S, C> traversalOption) {
        if (token == Merge.onCreate) {
            this.onCreateTraversal = this.integrateChild(traversalOption);
        } else if (token == Merge.onMatch) {
            this.onMatchTraversal = this.integrateChild(traversalOption);
        } else {
            throw new UnsupportedOperationException(String.format("Option %s for Merge is not supported", token.name()));
        }
    }

    @Override
    public <S, C> List<Traversal.Admin<S, C>> getLocalChildren() {
        final List<Traversal.Admin<S, C>> children = new ArrayList<>();
        if (mergeTraversal != null) children.add((Traversal.Admin<S, C>) mergeTraversal);
        if (onMatchTraversal != null) children.add((Traversal.Admin<S, C>) onMatchTraversal);
        if (onCreateTraversal != null) children.add((Traversal.Admin<S, C>) onCreateTraversal);
        return children;
    }

    /**
     * This implementation should only be used as a mechanism for supporting {@link PartitionStrategy}. Using this
     * with {@link GraphTraversal#with(String,Object)} will have an ill effect of simply acting like a call to
     * {@link GraphTraversal#property(Object, Object, Object...)}. No mutating steps currently support use of
     * {@link GraphTraversal#with(String,Object)} so perhaps it's best to not start with that now.
     */
    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(this, keyValues);
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    public boolean isUsingPartitionStrategy() {
        return usesPartitionStrategy;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        // when it's a start step a traverser needs to be created to kick off the traversal.
        if (isStart && first) {
            first = false;
            generateTraverser(false);
        }
        return super.processNextStart();
    }

    private void generateTraverser(final Object o) {
        final TraverserGenerator generator = this.getTraversal().getTraverserGenerator();
        this.addStart(generator.generate(o, (Step) this, 1L));
    }

    protected Graph getGraph() {
        return this.getTraversal().getGraph().get();
    }

    @Override
    public CallbackRegistry<Event> getMutatingCallbackRegistry() {
        if (null == callbackRegistry) callbackRegistry = new ListCallbackRegistry<>();
        return callbackRegistry;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (mergeTraversal != null)
            result ^= mergeTraversal.hashCode();
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
        mergeTraversal.reset();
        if (onCreateTraversal != null) onCreateTraversal.reset();
        if (onMatchTraversal != null) onMatchTraversal.reset();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, mergeTraversal, onCreateTraversal, onMatchTraversal);
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(mergeTraversal);
        this.integrateChild(onCreateTraversal);
        this.integrateChild(onMatchTraversal);
    }

    @Override
    public MergeElementStep<S, E, C> clone() {
        final MergeElementStep<S, E, C> clone = (MergeElementStep<S, E, C>) super.clone();
        clone.mergeTraversal = mergeTraversal.clone();
        clone.onCreateTraversal = onCreateTraversal != null ? onCreateTraversal.clone() : null;
        clone.onMatchTraversal = onMatchTraversal != null ? onMatchTraversal.clone() : null;
        return clone;
    }

    protected void validate(final Map map, final boolean ignoreTokens) {
        final Set allowedTokens = getAllowedTokens();
        validate(map, ignoreTokens, allowedTokens, this instanceof MergeVertexStep ? "mergeV" : "mergeE");
    }

    protected static void validate(final Map map, final boolean ignoreTokens, final Set allowedTokens, final String op) {
        if (null == map) return;

        ((Map<?,?>) map).entrySet().forEach(e -> {
            final Object k = e.getKey();
            final Object v = e.getValue();

            if (v == null) {
                throw new IllegalArgumentException(String.format("%s() does not allow null Map values - check: %s", op, k));
            }

            if (ignoreTokens) {
                if (!(k instanceof String)) {
                    throw new IllegalArgumentException(String.format("option(onMatch) expects keys in Map to be of String - check: %s", k));
                } else {
                    ElementHelper.validateProperty((String) k, v);
                }
            } else {
                if (!(k instanceof String) && !allowedTokens.contains(k)) {
                    throw new IllegalArgumentException(String.format(
                            "%s() and option(onCreate) args expect keys in Map to be either String or %s - check: %s",
                            op, allowedTokens, k));
                }
                if (k == T.label) {
                    if (!(GValue.instanceOf(v, GType.STRING))) {
                        throw new IllegalArgumentException(String.format(
                                "%s() and option(onCreate) args expect T.label value to be of String - found: %s", op,
                                v.getClass().getSimpleName()));
                    } else {
                        ElementHelper.validateLabel((String) v);
                    }
                }
                if (k == Direction.OUT && v instanceof Merge && v != Merge.outV) {
                    throw new IllegalArgumentException(String.format("Only Merge.outV token may be used for Direction.OUT, found: %s", v));
                }
                if (k == Direction.IN && v instanceof Merge && v != Merge.inV) {
                    throw new IllegalArgumentException(String.format("Only Merge.inV token may be used for Direction.IN, found: %s", v));
                }
                if (k instanceof String) {
                    ElementHelper.validateProperty((String) k, v);
                }
            }
        });
    }

    /**
     * Prohibit overrides to the existence criteria (id/label/from/to) in onCreate.
     */
    protected void validateNoOverrides(final Map<?,?> mergeMap, final Map<?,?> onCreateMap) {
        for (final Map.Entry e : onCreateMap.entrySet()) {
            final Object k = e.getKey();
            final Object v = e.getValue();
            if (mergeMap.containsKey(k) && !Objects.equals(v, mergeMap.get(k))) {
                throw new IllegalArgumentException(String.format(
                        "option(onCreate) cannot override values from merge() argument: (%s, %s)", k, v));
            }
        }
    }

    /**
     * null Map == empty Map
     */
    protected Map materializeMap(final Traverser.Admin<S> traverser, Traversal.Admin<S, ?> mapTraversal) {
        final Object o = TraversalUtil.apply(traverser, mapTraversal);
        Map map = GValue.getFrom(o);

        // PartitionStrategy uses parameters as a mechanism for setting the partition key. trying to be as specific
        // as possible here wrt parameters usage to avoid misuse
        if (usesPartitionStrategy) {
            map = null == map ? new LinkedHashMap() : map;
            for (Map.Entry<Object, List<Object>> entry : parameters.getRaw().entrySet()) {
                final Object k = entry.getKey();
                final List<Object> v = entry.getValue();
                map.put(k, v.get(0));
            }
        }

        return map == null ? new LinkedHashMap() : map;
    }

    /**
     * Translate the Map into a g.V() traversal against the supplied graph. Graph providers will presumably optimize
     * this traversal to use whatever indices are present and appropriate for efficiency.
     *
     * Callers are responsible for closing this iterator when finished.
     */
    protected CloseableIterator<Vertex> searchVertices(final Map search) {
        if (search == null)
            return CloseableIterator.empty();

        final Graph graph = getGraph();
        final Object id = search.get(T.id);
        final String label = (String) search.get(T.label);

        GraphTraversal t = searchVerticesTraversal(graph, id);
        t = searchVerticesLabelConstraint(t, label);
        t = searchVerticesPropertyConstraints(t, search);

        // this should auto-close the underlying traversal
        return CloseableIterator.of(t);
    }

    protected GraphTraversal searchVerticesTraversal(final Graph graph, final Object id) {
        return id != null ? graph.traversal().V(id) : graph.traversal().V();
    }

    protected GraphTraversal searchVerticesLabelConstraint(final GraphTraversal t, final String label) {
        return label != null ? t.hasLabel(label) : t;
    }

    protected GraphTraversal searchVerticesPropertyConstraints(GraphTraversal t, final Map search) {
        for (final Map.Entry e : ((Map<?,?>) search).entrySet()) {
            final Object k = e.getKey();
            if (!(k instanceof String)) continue;
            t = t.has((String) k, e.getValue());
        }
        return t;
    }

    @Override
    protected abstract Iterator<E> flatMap(final Traverser.Admin<S> traverser);

    protected abstract Set getAllowedTokens();

}
