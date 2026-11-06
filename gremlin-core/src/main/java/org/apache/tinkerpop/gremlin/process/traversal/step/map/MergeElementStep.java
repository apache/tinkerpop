/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.tinkerpop.gremlin.process.traversal.lambda.GValueConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValueHelper;
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
        implements MergeStepContract<S, E, C> {

    protected final boolean isStart;
    protected boolean first = true;
    protected Traversal.Admin<S, Map> mergeTraversal;
    protected Traversal.Admin<S, Map> onCreateTraversal = null;
    protected Traversal.Admin<S, Map<String, ?>> onMatchTraversal = null;

    protected CallbackRegistry<Event> callbackRegistry;

    private Map<Object, List<Object>> properties = new HashMap<>(); // Used to support PartitionStrategy

    protected boolean usesPartitionStrategy;

    public MergeElementStep(final Traversal.Admin traversal, final boolean isStart) {
        this(traversal, isStart, new IdentityTraversal<>());
    }

    public MergeElementStep(final Traversal.Admin traversal, final boolean isStart, final Map mergeMap) {
        this(traversal, isStart, new ConstantTraversal<>(mergeMap));
        validate(mergeMap, false);
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
    @Override
    public Traversal.Admin<S, Map> getMergeTraversal() {
        return mergeTraversal;
    }

    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be used to create elements that
     * do not match the search criteria of {@link #getMergeTraversal()}.
     */
    @Override
    public Traversal.Admin<S, Map> getOnCreateTraversal() {
        return onCreateTraversal;
    }

    /**
     * Gets the traversal that will be used to provide the {@code Map} that will be used to modify elements that
     * match the search criteria of {@link #getMergeTraversal()}.
     */
    @Override
    public Traversal.Admin<S, Map<String, ?>> getOnMatchTraversal() {
        return onMatchTraversal;
    }

    /**
     * Determines if this is a start step.
     */
    @Override
    public boolean isStart() {
        return isStart;
    }

    /**
     * Determine if this is the first pass through {@link #processNextStart()}.
     */
    @Override
    public boolean isFirst() {
        return first;
    }

    public CallbackRegistry<Event> getCallbackRegistry() {
        return callbackRegistry;
    }

    @Override
    public void addChildOption(final Merge token, final Traversal.Admin<S, C> traversalOption) {
        if (token == Merge.onCreate) {
            if (onCreateTraversal != null)
                throw new IllegalArgumentException("option(onCreate) can only be specified once per merge step");
            validateStaticNoOverrides(mergeTraversal, traversalOption);
            this.onCreateTraversal = this.integrateChild(traversalOption);
        } else if (token == Merge.onMatch) {
            if (onMatchTraversal != null)
                throw new IllegalArgumentException("option(onMatch) can only be specified once per merge step");
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

        // T.labels is the multi-label form of T.label wherever the step declares it among its allowed tokens. The two
        // are interchangeable, so supplying both in one Map is ambiguous.
        final boolean labelsAllowed = allowedTokens.contains(T.labels);
        if (labelsAllowed && map.containsKey(T.label) && map.containsKey(T.labels)) {
            throw new IllegalArgumentException(String.format(
                    "%s() does not allow both T.label and T.labels in the same Map", op));
        }

        ((Map<?,?>) map).entrySet().forEach(e -> {
            final Object k = e.getKey();
            final Object v = e.getValue();
            final boolean isLabelKey = k == T.label || (labelsAllowed && k == T.labels);

            if (ignoreTokens) {
                // onMatch may add labels to a matched vertex; mergeE validates the key here but rejects it later
                if (isLabelKey) {
                    ElementHelper.asLabelSet(v);
                } else if (!(k instanceof String)) {
                    throw new IllegalArgumentException(String.format("option(onMatch) expects keys in Map to be of String - check: %s", k));
                } else {
                    ElementHelper.validateProperty((String) k, v);
                }
            } else {
                // don't allow null values for enums: T, Direction, Merge
                if (k instanceof Enum && v == null) {
                    throw new IllegalArgumentException(String.format("%s() does not allow null Map values - check: %s", op, k));
                }

                if (!(k instanceof String) && !allowedTokens.contains(k)) {
                    throw new IllegalArgumentException(String.format(
                            "%s() and option(onCreate) args expect keys in Map to be either String or %s - check: %s",
                            op, allowedTokens, k));
                }
                if (isLabelKey) {
                    ElementHelper.asLabelSet(v);
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
     * Returns the Map with a {@link T#labels} entry moved to {@link T#label}, its single-token equivalent, so that
     * the search, create and onMatch paths only need to handle {@link T#label}. An empty label collection means no
     * label was given, so it is removed: the search then does not filter by label, creation applies the graph's
     * default, and an {@code onCreate} label is not treated as an override. The supplied Map is never mutated; a copy
     * is returned only when it changes. Expects a Map that has already passed {@link #validate}, which rejects one
     * holding both tokens.
     *
     * @since 4.0.0
     */
    protected static Map normalizeLabelsToken(final Map map) {
        if (map == null || (!map.containsKey(T.labels) && !isEmptyLabelSet(map.get(T.label))))
            return map;
        final Map normalized = new LinkedHashMap(map);
        if (normalized.containsKey(T.labels))
            normalized.put(T.label, normalized.remove(T.labels));
        if (isEmptyLabelSet(normalized.get(T.label)))
            normalized.remove(T.label);
        return normalized;
    }

    private static boolean isEmptyLabelSet(final Object labelValue) {
        return labelValue instanceof Collection && ((Collection<?>) labelValue).isEmpty();
    }

    /**
     * Prohibit overrides to the existence criteria (id/label/from/to) in onCreate. Labels may be given with
     * {@link T#label} or {@link T#labels} as a {@code String} or a collection, so they are compared as label sets,
     * and an empty label collection on either side counts as no label.
     */
    public static void validateNoOverrides(final Map<?,?> mergeMap, final Map<?,?> onCreateMap) {
        for (final Map.Entry e : onCreateMap.entrySet()) {
            final Object k = e.getKey();
            if (k == T.label || k == T.labels)
                continue;
            final Object v = e.getValue();
            if (mergeMap.containsKey(k) && !Objects.equals(v, mergeMap.get(k))) {
                throw new IllegalArgumentException(String.format(
                        "option(onCreate) cannot override values from merge() argument: (%s, %s)", k, v));
            }
        }

        final Object createLabel = labelValue(onCreateMap);
        final Object mergeLabel = labelValue(mergeMap);
        if (createLabel != null && mergeLabel != null && !isEmptyLabelSet(createLabel) && !isEmptyLabelSet(mergeLabel)
                && !sameLabels(mergeLabel, createLabel)) {
            throw new IllegalArgumentException(String.format(
                    "option(onCreate) cannot override values from merge() argument: (%s, %s)", T.label, createLabel));
        }
    }

    private static Object labelValue(final Map<?,?> map) {
        return map.containsKey(T.label) ? map.get(T.label) : map.get(T.labels);
    }

    /**
     * Compares two label values as label sets. A value that is not a valid label, such as a traversal, is compared
     * as is.
     */
    private static boolean sameLabels(final Object a, final Object b) {
        try {
            return ElementHelper.asLabelSet(a).equals(ElementHelper.asLabelSet(b));
        } catch (IllegalArgumentException ex) {
            return Objects.equals(a, b);
        }
    }

    /**
     * null Map == empty Map
     */
    protected Map materializeMap(final Traverser.Admin<S> traverser, Traversal.Admin<S, ?> mapTraversal) {
        Map map = (Map) TraversalUtil.apply(traverser, mapTraversal);

        // PartitionStrategy uses properties as a mechanism for setting the partition key.
        if (usesPartitionStrategy) {
            map = null == map ? new LinkedHashMap() : map;
            for (Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
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
        final Object labelValue = search.get(T.label);

        GraphTraversal t = searchVerticesTraversal(graph, id);
        t = searchVerticesLabelConstraint(t, labelValue);
        t = searchVerticesPropertyConstraints(t, search);

        // this should auto-close the underlying traversal
        return CloseableIterator.of(t);
    }

    protected GraphTraversal searchVerticesTraversal(final Graph graph, final Object id) {
        return id != null ? graph.traversal().V(id) : graph.traversal().V();
    }

    protected GraphTraversal searchVerticesLabelConstraint(GraphTraversal t, final Object labelValue) {
        if (labelValue == null) {
            return t;
        }
        if (labelValue instanceof String) {
            return t.hasLabel((String) labelValue);
        } else if (labelValue instanceof java.util.Collection) {
            // Multi-label: AND semantics - must have ALL specified labels
            for (final Object label : (java.util.Collection<?>) labelValue) {
                t = t.hasLabel((String) label);
            }
            return t;
        }
        return t;
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

    @Override
    public void setMerge(final Traversal.Admin<?,Map<Object, Object>> mergeTraversal) {
        this.mergeTraversal = integrateChild(mergeTraversal);
    }

    @Override
    public void setOnCreate(final Traversal.Admin<?,Map<Object, Object>> onCreateTraversal) {
        this.onCreateTraversal = integrateChild(onCreateTraversal);
    }

    @Override
    public void setOnMatch(final Traversal.Admin<?,Map<Object, Object>> onMatchTraversal) {
        this.onMatchTraversal = integrateChild(onMatchTraversal);
    }

    /**
     * This implementation should only be used as a mechanism for supporting {@link PartitionStrategy}.
     */
    @Override
    public void addProperty(Object key, Object value) {
        if (properties.containsKey(key)) {
            throw new IllegalArgumentException("MergeElement.addProperty only support properties with single cardinality");
        }
        properties.put(key, Collections.singletonList(value));
    }

    @Override
    public Map<Object, List<Object>> getProperties() {
        return GValueHelper.resolveProperties(properties);
    }


    @Override
    public boolean removeProperty(Object k) {
        if (properties.containsKey(k)) {
            properties.remove(k);
            return true;
        }
        return false;
    }

    public static void validateStaticNoOverrides(final Traversal.Admin search, final Traversal.Admin create) {
        final Object searchObj = constantValue(search);
        final Object createObj = constantValue(create);
        if (searchObj instanceof Map && createObj instanceof Map)
            validateNoOverrides((Map) searchObj, (Map) createObj);
    }

    /**
     * Returns the value of a constant-like option traversal, or {@code null} if it is not constant or is
     * parameterized, in which case it can only be validated when the step executes.
     */
    static Object constantValue(final Traversal.Admin traversal) {
        if (traversal instanceof ConstantTraversal)
            return traversal.next();
        if (traversal instanceof GValueConstantTraversal && !((GValueConstantTraversal) traversal).isParameterized())
            return traversal.next();
        return null;
    }
}
