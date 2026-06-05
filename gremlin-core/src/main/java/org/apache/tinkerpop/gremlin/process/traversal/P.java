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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ChildTraversalValidator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Predefined {@code Predicate} values that can be used to define filters to {@code has()} and {@code where()}.
 * 
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class P<V> implements Predicate<V>, Serializable, Cloneable {

    protected PBiPredicate<V, V> biPredicate;
    protected Map<String, V> variables = new HashMap<>();
    protected Collection<V> literals = Collections.EMPTY_LIST;
    private boolean isCollection = false;
    private boolean resolvedEmpty = false;
    private Traversal.Admin<?, ?> traversalValue;
    private List<Traversal.Admin<?, ?>> traversalValues;

    public P(final PBiPredicate<V, V> biPredicate, final V value) {
        this.biPredicate = biPredicate;
        // If the value is a DefaultGraphTraversal (the type created by __.xxx() anonymous traversals),
        // treat it as a child traversal rather than a literal. This handles the case where Java's
        // overload resolution picks P(BiPredicate, V) instead of P(BiPredicate, Traversal) when
        // the caller passes a GraphTraversal. We specifically check for DefaultGraphTraversal
        // rather than Traversal to avoid catching internal traversal types like ConstantTraversal,
        // ValueTraversal, and IdentityTraversal which are used as literal values in P.
        if (value instanceof org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal) {
            this.traversalValue = ((Traversal<?, ?>) value).asAdmin();
        } else {
            setValue(value);
        }
    }

    public P(final PBiPredicate<V, V> biPredicate, final GValue<V> value) {
        if (value != null) {
            if (value.isVariable()) {
                variables.put(value.getName(), value.get());
            } else {
                literals = Collections.singleton(value.get());
            }
        } else {
            this.literals = Collections.singleton(null);
        }
        this.isCollection = false;
        this.biPredicate = biPredicate;
    }

    protected P(final PBiPredicate<V, V> biPredicate, final Collection<V> literals, final Map<String, V> variables, final boolean isCollection) {
        this.biPredicate = biPredicate;
        this.variables.putAll(variables);
        if (literals instanceof BulkSet) {
            this.literals = new BulkSet<>();
            this.literals.addAll(literals);
        } else {
            this.literals = new ArrayList<>(literals);
        }
        this.isCollection = isCollection;
    }

    /**
     * Constructs a {@code P} with a child traversal whose result is resolved at runtime against the current
     * traverser. The literals and variables are left at their defaults and will be populated when
     * {@link #resolve(Traverser.Admin)} is called.
     */
    public P(final PBiPredicate<V, V> biPredicate, final Traversal.Admin<?, ?> traversalValue) {
        this.biPredicate = biPredicate;
        this.traversalValue = traversalValue;
    }

    /**
     * Constructs a {@code P} with multiple child traversals whose results are unioned at runtime against the
     * current traverser. Only valid for collection predicates ({@link Contains#within}, {@link Contains#without}).
     * The literals and variables are left at their defaults and will be populated when
     * {@link #resolve(Traverser.Admin)} is called.
     *
     * @since 4.0.0
     */
    public P(final PBiPredicate<V, V> biPredicate, final List<Traversal.Admin<?, ?>> traversalValues) {
        this.biPredicate = biPredicate;
        this.traversalValues = traversalValues;
    }

    public PBiPredicate<V, V> getBiPredicate() {
        return this.biPredicate;
    }

    /**
     * Get the name of the predicate
     */
    public String getPredicateName() { return biPredicate.getPredicateName(); }

    /**
     * Gets the current value to be passed to the predicate for testing.
     */
    public V getValue() {
        if (isCollection) {
            if (this.variables.isEmpty()) return (V) this.literals;

            final Collection<V> values;
            if (this.literals instanceof BulkSet) {
                values = new BulkSet<>();
                values.addAll(this.literals);
            } else {
                values = new ArrayList<>(this.literals);
            }

            values.addAll(this.variables.values());
            return (V) values;
        } else if (!this.literals.isEmpty()) {
            return this.literals.iterator().next();
        } else if (!this.variables.isEmpty()) {
            return this.variables.values().iterator().next();
        }
        return null;
    }

    public void setValue(final V value) {
        variables.clear();
        literals = Collections.EMPTY_LIST;

        if (value == null) {
            isCollection = false;
            this.literals = Collections.singleton(null);
        } else if (value instanceof GValue) {
            variables.put(((GValue<V>) value).getName(), ((GValue<V>) value).get());
            isCollection = false;
        } else if (value instanceof Collection) {
            isCollection = true;
            if (((Collection<?>) value).stream().anyMatch(v -> v instanceof GValue)) {
                this.literals = (value instanceof BulkSet) ? new BulkSet<>() : new ArrayList<>();
                for (Object v : ((Collection<?>) value)) {
                    // Separate variables and literals
                    if (v instanceof GValue) {
                        if (((GValue<V>) v).isVariable()) {
                            variables.put(((GValue<V>) v).getName(), ((GValue<V>) v).get());
                        } else {
                            literals.add(((GValue<V>) v).get());
                        }
                    } else {
                        literals.add((V) v);
                    }
                }
            } else {
                literals = (Collection<V>) value; // Retain original collection when possible
            }
        } else {
            isCollection = false;
            this.literals = Collections.singleton(value);
        }
    }

    @Override
    public boolean test(final V testValue) {
        return this.biPredicate.test(testValue, this.getValue());
    }

    @Override
    public int hashCode() {
        int result = this.biPredicate.hashCode();
        if (null != this.variables)
            result ^= this.variables.hashCode();
        if (null != this.literals)
            result ^= this.literals.hashCode();
        if (null != this.traversalValue)
            result ^= this.traversalValue.hashCode();
        if (null != this.traversalValues)
            result ^= this.traversalValues.hashCode();
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof P &&
                ((P) other).getClass().equals(this.getClass()) &&
                ((P) other).getBiPredicate().equals(this.biPredicate) &&
                ((((P) other).variables == null && this.variables == null) || (((P) other).variables != null && ((P) other).variables.equals(this.variables))) &&
                ((((P) other).literals == null && this.literals == null) || (((P) other).literals != null && CollectionUtils.isEqualCollection(((P) other).literals, this.literals))) &&
                Objects.equals(((P) other).traversalValue, this.traversalValue) &&
                Objects.equals(((P) other).traversalValues, this.traversalValues);
    }

    @Override
    public String toString() {
        return null == this.getValue() ? this.biPredicate.toString() : this.biPredicate.toString() + "(" + this.getValue() + ")";
    }

    @Override
    public P<V> negate() {
        return new NotP<>(this);
    }

    @Override
    public P<V> and(final Predicate<? super V> predicate) {
        if (!(predicate instanceof P))
            throw new IllegalArgumentException("Only P predicates can be and'd together");
        return new AndP<>(Arrays.asList(this, (P<V>) predicate));
    }

    @Override
    public P<V> or(final Predicate<? super V> predicate) {
        if (!(predicate instanceof P))
            throw new IllegalArgumentException("Only P predicates can be or'd together");
        return new OrP<>(Arrays.asList(this, (P<V>) predicate));
    }

    public P<V> clone() {
        try {
            final P<V> clone = (P<V>) super.clone();
            if (this.traversalValue != null) {
                clone.traversalValue = this.traversalValue.clone();
            }
            if (this.traversalValues != null) {
                clone.traversalValues = new ArrayList<>(this.traversalValues.size());
                for (final Traversal.Admin<?, ?> tv : this.traversalValues) {
                    clone.traversalValues.add(tv.clone());
                }
            }
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public boolean isParameterized() {
        return !variables.isEmpty();
    }

    public void updateVariable(final String name, final Object value) {
        if (variables.containsKey(name)) {
            variables.put(name, (V) value);
        }
    }

    public Set<GValue<?>> getGValues() {
        Set<GValue<?>> results = new HashSet<>();
        for (Map.Entry<String, V> entry : variables.entrySet()) {
            results.add(GValue.of(entry.getKey(), entry.getValue()));
        }
        return results;
    }

    /**
     * Determines if this predicate holds a child traversal whose result is resolved at runtime.
     */
    public boolean hasTraversal() {
        return this.traversalValue != null || (this.traversalValues != null && !this.traversalValues.isEmpty());
    }

    /**
     * Returns {@code true} if the most recent call to {@link #resolve(Traverser.Admin)} produced no results.
     * Steps should check this after calling {@code resolve()} and short-circuit appropriately rather than
     * calling {@link #test(Object)}, which would compare against {@code null}.
     */
    public boolean isResolvedEmpty() {
        return this.resolvedEmpty;
    }

    /**
     * Gets the child traversal value, if one was provided. Returns {@code null} when this predicate uses
     * literal values or variables.
     */
    public Traversal.Admin<?, ?> getTraversalValue() {
        return this.traversalValue;
    }

    /**
     * Gets the list of child traversal values for multi-traversal predicates (e.g., {@code within(trav1, trav2)}).
     * Returns {@code null} when this predicate uses a single traversal or literal values.
     *
     * @since 4.0.0
     */
    public List<Traversal.Admin<?, ?>> getTraversalValues() {
        return this.traversalValues;
    }

    /**
     * Resolves the child traversal against the given traverser, replacing the traversal value with the
     * resolved literal(s) for this test cycle. If no traversal is present, this method returns immediately.
     *
     * <p>For all predicates, only the first result from the child traversal is used,
     * consistent with {@code by(traversal)} semantics. For collection predicates
     * ({@link Contains#within}, {@link Contains#without}), the first result should be a
     * {@link Collection} (e.g., produced by {@code fold()}).</p>
     *
     * <p>When multiple traversals are present (via {@link #traversalValues}), each traversal is evaluated
     * independently, the first result from each is taken, and results are combined into a single collection.
     * This is only valid for collection predicates ({@link Contains#within}, {@link Contains#without}).</p>
     */
    @SuppressWarnings("unchecked")
    public void resolve(final Traverser.Admin<?> traverser) {
        if (this.traversalValues != null && !this.traversalValues.isEmpty()) {
            resolveMultipleTraversals(traverser);
            return;
        }

        if (this.traversalValue == null) return;

        final Traversal.Admin<Object, Object> trav = (Traversal.Admin<Object, Object>) (Traversal.Admin) this.traversalValue;
        final Traverser.Admin<Object> split = (Traverser.Admin<Object>) traverser.split();
        split.setSideEffects(trav.getSideEffects());
        split.setBulk(1L);
        trav.reset();
        trav.addStart(split);

        try {
            // Take first result only — consistent with by(traversal) semantics.
            if (!trav.hasNext()) {
                this.resolvedEmpty = true;
                this.literals = Collections.emptyList();
                this.isCollection = false;
            } else {
                this.resolvedEmpty = false;
                final Object firstResult = trav.next();
                if (this.biPredicate instanceof Contains) {
                    // Contains predicates need a Collection value. If first result is already
                    // a Collection (e.g., from fold()), use it directly. Otherwise wrap in singleton.
                    if (firstResult instanceof Collection) {
                        this.literals = (Collection<V>) firstResult;
                    } else {
                        this.literals = Collections.singletonList((V) firstResult);
                    }
                    this.isCollection = true;
                } else {
                    setValue((V) firstResult);
                }
            }
        } finally {
            CloseableIterator.closeIterator(trav);
        }
    }

    /**
     * Resolves multiple child traversals, taking the first result from each and combining into a collection.
     * Only valid for collection predicates ({@link Contains}).
     */
    @SuppressWarnings("unchecked")
    private void resolveMultipleTraversals(final Traverser.Admin<?> traverser) {
        final List<Object> allResults = new ArrayList<>();

        for (final Traversal.Admin<?, ?> tv : this.traversalValues) {
            final Traversal.Admin<Object, Object> trav = (Traversal.Admin<Object, Object>) (Traversal.Admin) tv;
            final Traverser.Admin<Object> split = (Traverser.Admin<Object>) traverser.split();
            split.setSideEffects(trav.getSideEffects());
            split.setBulk(1L);
            trav.reset();
            trav.addStart(split);

            try {
                // Take first result only from each traversal.
                // If the result is a Collection (from fold()), unpack it.
                if (trav.hasNext()) {
                    final Object firstResult = trav.next();
                    if (firstResult instanceof Collection) {
                        allResults.addAll((Collection<?>) firstResult);
                    } else {
                        allResults.add(firstResult);
                    }
                }
            } finally {
                CloseableIterator.closeIterator(trav);
            }
        }

        this.resolvedEmpty = allResults.isEmpty();
        if (allResults.isEmpty()) {
            this.literals = Collections.emptyList();
            this.isCollection = false;
        } else {
            this.literals = (Collection<V>) (Collection<?>) allResults;
            this.isCollection = true;
        }
    }

    //////////////// predicate traversal utilities

    /**
     * Recursively integrates all child traversals found in the predicate tree into the given parent step.
     * Handles {@link ConnectiveP} (recurses into children) and {@link NotP} (recurses into wrapped predicate).
     */
    public static void integrateTraversals(final P<?> p, final TraversalParent parent) {
        if (p instanceof ConnectiveP) {
            for (final P<?> child : ((ConnectiveP<?>) p).getPredicates()) {
                integrateTraversals(child, parent);
            }
        } else if (p instanceof NotP) {
            integrateTraversals(((NotP<?>) p).negate(), parent);
        } else if (p.getTraversalValue() != null) {
            parent.integrateChild(p.getTraversalValue());
        } else if (p.getTraversalValues() != null) {
            for (final Traversal.Admin<?, ?> tv : p.getTraversalValues()) {
                parent.integrateChild(tv);
            }
        }
    }

    /**
     * Recursively collects all child traversals from a predicate tree.
     * Handles {@link ConnectiveP} (recurses into children) and {@link NotP} (recurses into wrapped predicate).
     */
    public static void collectTraversals(final P<?> p, final List<Traversal.Admin<?, ?>> traversals) {
        if (p instanceof ConnectiveP) {
            for (final P<?> child : ((ConnectiveP<?>) p).getPredicates()) {
                collectTraversals(child, traversals);
            }
        } else if (p instanceof NotP) {
            collectTraversals(((NotP<?>) p).negate(), traversals);
        } else if (p.getTraversalValue() != null) {
            traversals.add(p.getTraversalValue());
        } else if (p.getTraversalValues() != null) {
            traversals.addAll(p.getTraversalValues());
        }
    }

    //////////////// statics

    /**
     * Determines if values are equal.
     *
     * @since 3.0.0-incubating
     */
    public static <V> P<V> eq(final V value) {
        return new P(Compare.eq, value);
    }
    /**
     * Determines if values are equal.
     *
     * @since 3.8.0
     */
    public static <V> P<V> eq(final GValue<V> value) {
        return new P(Compare.eq, value);
    }

    /**
     * Determines if values are not equal.
     *
     * @since 3.0.0-incubating
     */
    public static <V> P<V> neq(final V value) {
        return new P(Compare.neq, value);
    }

    /**
     * Determines if values are not equal.
     *
     * @since 3.8.0
     */
    public static <V> P<V> neq(final GValue<V> value) {
        return new P(Compare.neq, value);
    }

    /**
     * Determines if a value is less than another.
     *
     * @since 3.0.0-incubating
     */
    public static <V> P<V> lt(final V value) {
        return new P(Compare.lt, value);
    }

    /**
     * Determines if a value is less than another.
     *
     * @since 3.8.0
     */
    public static <V> P<V> lt(final GValue<V> value) {
        return new P(Compare.lt, value);
    }

    /**
     * Determines if a value is less than or equal to another.
     *
     * @since 3.0.0-incubating
     */
    public static <V> P<V> lte(final V value) {
        return new P(Compare.lte, value);
    }

    /**
     * Determines if a value is less than or equal to another.
     *
     * @since 3.8.0
     */
    public static <V> P<V> lte(final GValue<V> value) {
        return new P(Compare.lte, value);
    }

    /**
     * Determines if a value is greater than another.
     *
     * @since 3.0.0-incubating
     */
    public static <V> P<V> gt(final V value) {
        return new P(Compare.gt, value);
    }

    /**
     * Determines if a value is greater than another.
     *
     * @since 3.8.0
     */
    public static <V> P<V> gt(final GValue<V> value) {
        return new P(Compare.gt, value);
    }

    /**
     * Determines if a value is greater than or equal to another.
     *
     * @since 3.0.0-incubating
     */
    public static <V> P<V> gte(final V value) {
        return new P(Compare.gte, value);
    }

    /**
     * Determines if a value is greater than or equal to another.
     *
     * @since 3.8.0
     */
    public static <V> P<V> gte(final GValue<V> value) {
        return new P(Compare.gte, value);
    }

    /**
     * Determines if a value is within (exclusive) the range of the two specified values.
     *
     * @since 3.0.0-incubating
     */
    public static <V> P<V> inside(final V first, final V second) {
        return new AndP<V>(Arrays.asList(new P(Compare.gt, first), new P(Compare.lt, second)));
    }

    /**
     * Determines if a value is within (exclusive) the range of the two specified values.
     *
     * @since 3.8.0
     */
    public static <V> P<V> inside(final GValue<V> first, final GValue<V> second) {
        return new AndP<V>(Arrays.asList(new P(Compare.gt, first), new P(Compare.lt, second)));
    }

    /**
     * Determines if a value is not within (exclusive) of the range of the two specified values.
     *
     * @since 3.0.0-incubating
     */
    public static <V> P<V> outside(final V first, final V second) {
        return new OrP<V>(Arrays.asList(new P(Compare.lt, first), new P(Compare.gt, second)));
    }

    /**
     * Determines if a value is not within (exclusive) of the range of the two specified values.
     *
     * @since 3.8.0
     */
    public static <V> P<V> outside(final GValue<V> first, final GValue<V> second) {
        return new OrP<V>(Arrays.asList(new P(Compare.lt, first), new P(Compare.gt, second)));
    }

    /**
     * Determines if a value is within (inclusive) of the range of the two specified values.
     *
     * @since 3.0.0-incubating
     */
    public static <V> P<V> between(final V first, final V second) {
        return new AndP<V>(Arrays.asList(new P(Compare.gte, first), new P(Compare.lt, second)));
    }

    /**
     * Determines if a value is within (inclusive) of the range of the two specified values.
     *
     * @since 3.8.0
     */
    public static <V> P<V> between(final GValue<V> first, final GValue<V> second) {
        return new AndP<V>(Arrays.asList(new P(Compare.gte, first), new P(Compare.lt, second)));
    }

    /**
     * Determines if a value is within the specified list of values. If the array of arguments itself is {@code null}
     * then the argument is treated as {@code Object[1]} where that single value is {@code null}.
     *
     * @since 3.0.0-incubating
     */
    public static <V> P<V> within(final V... values) {
        // If a single Traversal is passed, redirect to the Traversal-accepting overload.
        // This handles cases where Java's overload resolution picks the varargs method
        // instead of the Traversal-specific method.
        if (values != null && values.length == 1 && values[0] instanceof Traversal) {
            return P.within((Traversal<?, ?>) values[0]);
        }
        // If multiple Traversals are passed, redirect to the multi-traversal overload.
        if (values != null && values.length > 1 && allTraversals(values)) {
            final List<Traversal.Admin<?, ?>> traversals = new ArrayList<>(values.length);
            for (final V v : values) {
                traversals.add(((Traversal<?, ?>) v).asAdmin());
            }
            return new P(Contains.within, traversals);
        }
        // Reject mixed traversals and literals — would silently produce wrong results.
        if (values != null && values.length > 1 && anyTraversals(values)) {
            throw new IllegalArgumentException(
                    "Cannot mix traversals and literal values in within(). " +
                    "Use within(__.constant(val1), __.constant(val2)) to wrap all values as traversals.");
        }
        final V[] v = null == values ? (V[]) new Object[] { null } : (V[]) values;
        return P.within(Arrays.asList(v));
    }

    /**
     * Determines if a value is within the specified list of values. If the array of arguments itself is {@code null}
     * then the argument is treated as {@code Object[1]} where that single value is {@code null}.
     *
     * @since 3.8.0
     */
    public static <V> P<V> within(final GValue<V>... values) {
        final V[] v = null == values ? (V[]) new Object[] { null } : (V[]) values;
        return P.within(Arrays.asList(v));
    }

    /**
     * Determines if a value is within the specified list of values. Calling this with {@code null} is the same as
     * calling {@link #within(Object[])} using {@code null}.
     *
     * @since 3.0.0-incubating
     */
    public static <V> P<V> within(final Collection<V> value) {
        if (null == value) return P.within((V) null);
        return new P(Contains.within, value);
    }

    /**
     * Determines if a value is not within the specified list of values. If the array of arguments itself is {@code null}
     * then the argument is treated as {@code Object[1]} where that single value is {@code null}.
     *
     * @since 3.0.0-incubating
     */
    public static <V> P<V> without(final V... values) {
        // If a single Traversal is passed, redirect to the Traversal-accepting overload.
        if (values != null && values.length == 1 && values[0] instanceof Traversal) {
            return P.without((Traversal<?, ?>) values[0]);
        }
        // If multiple Traversals are passed, redirect to the multi-traversal overload.
        if (values != null && values.length > 1 && allTraversals(values)) {
            final List<Traversal.Admin<?, ?>> traversals = new ArrayList<>(values.length);
            for (final V v : values) {
                traversals.add(((Traversal<?, ?>) v).asAdmin());
            }
            return new P(Contains.without, traversals);
        }
        // Reject mixed traversals and literals
        if (values != null && values.length > 1 && anyTraversals(values)) {
            throw new IllegalArgumentException(
                    "Cannot mix traversals and literal values in without(). " +
                    "Use without(__.constant(val1), __.constant(val2)) to wrap all values as traversals.");
        }
        final V[] v = null == values ? (V[]) new Object[] { null } : values;
        return P.without(Arrays.asList(v));
    }

    /**
     * Determines if a value is not within the specified list of values. If the array of arguments itself is {@code null}
     * then the argument is treated as {@code Object[1]} where that single value is {@code null}.
     *
     * @since 3.8.0
     */
    public static <V> P<V> without(final GValue<V>... values) {
        final V[] v = null == values ? (V[]) new Object[] { null } : (V[]) values;
        return P.without(Arrays.asList(v));
    }

    /**
     * Determines if a value is not within the specified list of values. Calling this with {@code null} is the same as
     * calling {@link #within(Object[])} using {@code null}.
     *
     * @since 3.0.0-incubating
     */
    public static <V> P<V> without(final Collection<V> value) {
        if (null == value) return P.without((V) null);
        return new P(Contains.without, value);
    }

    /**
     * Determines if values are equal using a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> eq(final Traversal<?, ?> traversalValue) {
        ChildTraversalValidator.validate(traversalValue.asAdmin());
        return new P(Compare.eq, traversalValue.asAdmin());
    }

    /**
     * Determines if values are not equal using a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> neq(final Traversal<?, ?> traversalValue) {
        ChildTraversalValidator.validate(traversalValue.asAdmin());
        return new P(Compare.neq, traversalValue.asAdmin());
    }

    /**
     * Determines if a value is less than another using a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> lt(final Traversal<?, ?> traversalValue) {
        ChildTraversalValidator.validate(traversalValue.asAdmin());
        return new P(Compare.lt, traversalValue.asAdmin());
    }

    /**
     * Determines if a value is less than or equal to another using a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> lte(final Traversal<?, ?> traversalValue) {
        ChildTraversalValidator.validate(traversalValue.asAdmin());
        return new P(Compare.lte, traversalValue.asAdmin());
    }

    /**
     * Determines if a value is greater than another using a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> gt(final Traversal<?, ?> traversalValue) {
        ChildTraversalValidator.validate(traversalValue.asAdmin());
        return new P(Compare.gt, traversalValue.asAdmin());
    }

    /**
     * Determines if a value is greater than or equal to another using a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> gte(final Traversal<?, ?> traversalValue) {
        ChildTraversalValidator.validate(traversalValue.asAdmin());
        return new P(Compare.gte, traversalValue.asAdmin());
    }

    /**
     * Determines if a value is within the results of a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> within(final Traversal<?, ?> traversalValue) {
        ChildTraversalValidator.validate(traversalValue.asAdmin());
        return new P(Contains.within, traversalValue.asAdmin());
    }

    /**
     * Determines if a value is within the union of results from multiple child traversals resolved at runtime.
     * Each traversal is evaluated independently and results are combined into a single collection.
     *
     * @since 4.0.0
     */
    public static <V> P<V> within(final Traversal<?, ?> first, final Traversal<?, ?> second, final Traversal<?, ?>... more) {
        final List<Traversal.Admin<?, ?>> traversals = new ArrayList<>(2 + (more != null ? more.length : 0));
        traversals.add(first.asAdmin());
        traversals.add(second.asAdmin());
        if (more != null) {
            for (final Traversal<?, ?> tv : more) {
                traversals.add(tv.asAdmin());
            }
        }
        for (final Traversal.Admin<?, ?> tv : traversals) {
            ChildTraversalValidator.validate(tv);
        }
        return new P(Contains.within, traversals);
    }

    /**
     * Determines if a value is not within the results of a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> without(final Traversal<?, ?> traversalValue) {
        ChildTraversalValidator.validate(traversalValue.asAdmin());
        return new P(Contains.without, traversalValue.asAdmin());
    }

    /**
     * Determines if a value is not within the union of results from multiple child traversals resolved at runtime.
     * Each traversal is evaluated independently and results are combined into a single collection.
     *
     * @since 4.0.0
     */
    public static <V> P<V> without(final Traversal<?, ?> first, final Traversal<?, ?> second, final Traversal<?, ?>... more) {
        final List<Traversal.Admin<?, ?>> traversals = new ArrayList<>(2 + (more != null ? more.length : 0));
        traversals.add(first.asAdmin());
        traversals.add(second.asAdmin());
        if (more != null) {
            for (final Traversal<?, ?> tv : more) {
                traversals.add(tv.asAdmin());
            }
        }
        for (final Traversal.Admin<?, ?> tv : traversals) {
            ChildTraversalValidator.validate(tv);
        }
        return new P(Contains.without, traversals);
    }

    /**
     * Determines if a value is of a type denoted by {@code GType}.
     *
     * @since 3.8.0
     */
    public static <V> P<V> typeOf(final GType value) {
        return new P(CompareType.typeOf, value);
    }

    /**
     * Determines if a value is of a type denoted by String key of GlobalTypeCache.
     *
     * @since 3.8.0
     */
    public static <V> P<V> typeOf(final String value) {
        return new P(CompareType.typeOf, value);
    }

    /**
     * Sugar method for Java/Groovy embedded cases only, determines if a value is of a type denoted by class.
     *
     * @since 3.8.0
     */
    public static <V> P<V> typeOf(final Class<?> value) {
        return new P(CompareType.typeOf, value);
    }

    /**
     * Construct an instance of {@code P} from a {@code BiPredicate}.
     *
     * @since 3.0.0-incubating
     */
    public static P test(final PBiPredicate biPredicate, final Object value) {
        return new P(biPredicate, value);
    }

    /**
     * The opposite of the specified {@code P}.
     *
     * @since 3.0.0-incubating
     */
    public static <V> P<V> not(final P<V> predicate) {
        return predicate.negate();
    }

    /**
     * Checks if all elements in the array are {@link Traversal} instances (specifically
     * {@link org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal}).
     */
    private static <V> boolean allTraversals(final V[] values) {
        for (final V v : values) {
            if (!(v instanceof org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if any element in the array is a {@link Traversal} instance (specifically
     * {@link org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal}).
     */
    private static <V> boolean anyTraversals(final V[] values) {
        for (final V v : values) {
            if (v instanceof org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal) {
                return true;
            }
        }
        return false;
    }
}
