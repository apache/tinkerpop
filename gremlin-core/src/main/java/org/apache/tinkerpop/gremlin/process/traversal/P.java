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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ChildTraversalValidator;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

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
    private List<Traversal.Admin<?, ?>> childTraversals;

    public P(final PBiPredicate<V, V> biPredicate, final V value) {
        this.biPredicate = biPredicate;
        setValue(value);
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
    @SuppressWarnings("unchecked")
    public P(final PBiPredicate<V, V> biPredicate, final Traversal.Admin<?, ?> traversalValue) {
        this.biPredicate = biPredicate;
        setValue((V) traversalValue);
    }

    /**
     * Constructs a {@code P} with multiple child traversals whose results are unioned at runtime against the
     * current traverser. Only valid for collection predicates ({@link Contains#within}, {@link Contains#without}).
     * The literals and variables are left at their defaults and will be populated when
     * {@link #resolve(Traverser.Admin)} is called.
     *
     * @since 4.0.0
     */
    @SuppressWarnings("unchecked")
    public P(final PBiPredicate<V, V> biPredicate, final List<Traversal.Admin<?, ?>> traversalValues) {
        this.biPredicate = biPredicate;
        setValue((V) traversalValues);
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
        // Full reset — completely replace the predicate's value state.
        this.childTraversals = null;
        this.variables.clear();
        this.literals = Collections.EMPTY_LIST;
        this.isCollection = false;
        applyValue(value);
    }

    /**
     * Core value-processing logic. Interprets the given value and populates the appropriate internal
     * fields ({@code childTraversals}, {@code literals}, {@code variables}, {@code isCollection}).
     * Does NOT reset state first — the caller is responsible for ensuring a clean slate if needed.
     */
    @SuppressWarnings("unchecked")
    private void applyValue(final V value) {
        if (value instanceof GraphTraversal) {
            final Traversal.Admin<?, ?> admin = ((Traversal<?, ?>) value).asAdmin();
            ChildTraversalValidator.validate(admin);
            this.childTraversals = Collections.singletonList(admin);
        } else if (value == null) {
            this.literals = Collections.singleton(null);
        } else if (value instanceof GValue) {
            variables.put(((GValue<V>) value).getName(), ((GValue<V>) value).get());
        } else if (value instanceof Collection) {
            final Collection<?> coll = (Collection<?>) value;
            if (!coll.isEmpty() && coll.stream().anyMatch(v -> v instanceof GraphTraversal)) {
                if (!coll.stream().allMatch(v -> v instanceof GraphTraversal)) {
                    throw new IllegalArgumentException(
                            "Cannot mix traversals and literal values. " +
                            "Use __.constant(val) to wrap all values as traversals.");
                }
                final List<Traversal.Admin<?, ?>> admins = new ArrayList<>(coll.size());
                for (final Object t : coll) {
                    final Traversal.Admin<?, ?> admin = ((Traversal<?, ?>) t).asAdmin();
                    ChildTraversalValidator.validate(admin);
                    admins.add(admin);
                }
                this.childTraversals = admins;
                this.isCollection = true;
            } else if (coll.stream().anyMatch(v -> v instanceof GValue)) {
                isCollection = true;
                this.literals = (value instanceof BulkSet) ? new BulkSet<>() : new ArrayList<>();
                for (final Object v : coll) {
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
                isCollection = true;
                literals = (Collection<V>) value;
            }
        } else {
            this.literals = Collections.singleton(value);
        }
    }

    @Override
    public boolean test(final V testValue) {
        if (this.resolvedEmpty) return false;
        return this.biPredicate.test(testValue, this.getValue());
    }

    @Override
    public int hashCode() {
        int result = this.biPredicate.hashCode();
        if (null != this.variables)
            result ^= this.variables.hashCode();
        if (null != this.literals)
            result ^= this.literals.hashCode();
        if (null != this.childTraversals)
            result ^= this.childTraversals.hashCode();
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof P &&
                ((P) other).getClass().equals(this.getClass()) &&
                ((P) other).getBiPredicate().equals(this.biPredicate) &&
                ((((P) other).variables == null && this.variables == null) || (((P) other).variables != null && ((P) other).variables.equals(this.variables))) &&
                ((((P) other).literals == null && this.literals == null) || (((P) other).literals != null && CollectionUtils.isEqualCollection(((P) other).literals, this.literals))) &&
                Objects.equals(((P) other).childTraversals, this.childTraversals);
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
            if (this.childTraversals != null) {
                clone.childTraversals = new ArrayList<>(this.childTraversals.size());
                for (final Traversal.Admin<?, ?> tv : this.childTraversals) {
                    clone.childTraversals.add(tv.clone());
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
        return this.childTraversals != null && !this.childTraversals.isEmpty();
    }

    /**
     * Returns {@code true} if the most recent call to {@link #resolve(Traverser.Admin)} produced no results
     * for a scalar predicate, meaning there is no meaningful comparison value.
     */
    public boolean isResolvedEmpty() {
        return this.resolvedEmpty;
    }

    /**
     * Gets the child traversals held by this predicate. Returns {@code null} when this predicate uses
     * literal values or variables.
     *
     * @since 4.0.0
     */
    public List<Traversal.Admin<?, ?>> getChildTraversals() {
        return this.childTraversals;
    }

    /**
     * Resolves the child traversal(s) against the given traverser, replacing the traversal value with the
     * resolved literal(s) for this test cycle. If no traversal is present, this method returns immediately.
     */
    @SuppressWarnings("unchecked")
    public void resolve(final Traverser.Admin<?> traverser) {
        if (this.childTraversals == null || this.childTraversals.isEmpty()) return;

        // Evaluate each child traversal, taking the first result from each.
        final List<Object> results = new ArrayList<>();
        for (final Traversal.Admin<?, ?> tv : this.childTraversals) {
            prepareChildTraversal(traverser, (Traversal.Admin<Object, Object>) tv);
            try {
                if (tv.hasNext()) {
                    results.add(tv.next());
                }
            } finally {
                CloseableIterator.closeIterator(tv);
            }
        }

        // Empty result: for collection predicates resolve to empty set; for scalar predicates short-circuit.
        if (results.isEmpty()) {
            if (this.isCollection) {
                this.resolvedEmpty = false;
                this.variables.clear();
                this.literals = Collections.emptyList();
            } else {
                this.resolvedEmpty = true;
            }
            return;
        }

        this.resolvedEmpty = false;
        this.variables.clear();
        this.literals = Collections.EMPTY_LIST;

        if (this.isCollection) {
            // Collection predicate (within, without) — flatten any Collection results into one list.
            final List<Object> flattened = new ArrayList<>();
            for (final Object r : results) {
                if (r instanceof Collection) {
                    flattened.addAll((Collection<?>) r);
                } else {
                    flattened.add(r);
                }
            }
            applyValue((V) flattened);
        } else {
            // Scalar predicate (eq, gt, etc.) — pass single result directly.
            applyValue((V) results.get(0));
        }
    }

    /**
     * Prepares a child traversal for evaluation by splitting the current traverser and seeding it.
     */
    @SuppressWarnings("unchecked")
    private static void prepareChildTraversal(final Traverser.Admin<?> traverser, final Traversal.Admin<Object, Object> trav) {
        final Traverser.Admin<Object> split = (Traverser.Admin<Object>) traverser.split();
        split.setSideEffects(trav.getSideEffects());
        split.setBulk(1L);
        trav.reset();
        trav.addStart(split);
    }

    //////////////// predicate traversal utilities

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
            collectTraversals(((NotP<?>) p).getWrapped(), traversals);
        } else if (p.getChildTraversals() != null) {
            traversals.addAll(p.getChildTraversals());
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
     * Determines if values are equal using a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> eq(final Traversal<?, ?> traversalValue) {
        return new P(Compare.eq, traversalValue.asAdmin());
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
     * Determines if values are not equal using a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> neq(final Traversal<?, ?> traversalValue) {
        return new P(Compare.neq, traversalValue.asAdmin());
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
     * Determines if a value is less than another using a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> lt(final Traversal<?, ?> traversalValue) {
        return new P(Compare.lt, traversalValue.asAdmin());
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
     * Determines if a value is less than or equal to another using a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> lte(final Traversal<?, ?> traversalValue) {
        return new P(Compare.lte, traversalValue.asAdmin());
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
     * Determines if a value is greater than another using a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> gt(final Traversal<?, ?> traversalValue) {
        return new P(Compare.gt, traversalValue.asAdmin());
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
     * Determines if a value is greater than or equal to another using a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> gte(final Traversal<?, ?> traversalValue) {
        return new P(Compare.gte, traversalValue.asAdmin());
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
     * Determines if a value is within the results of a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> within(final Traversal<?, ?> traversalValue) {
        return new P(Contains.within, Collections.singletonList(traversalValue.asAdmin()));
    }

    /**
     * Determines if a value is within the results of child traversals resolved at runtime.
     * Each traversal is evaluated independently and results are combined into a single collection.
     *
     * @since 4.0.0
     */
    public static <V> P<V> within(final Traversal<?, ?> first, final Traversal<?, ?>... more) {
        final List<Traversal.Admin<?, ?>> admins = new ArrayList<>(1 + more.length);
        admins.add(first.asAdmin());
        for (final Traversal<?, ?> tv : more) {
            admins.add(tv.asAdmin());
        }
        return new P(Contains.within, admins);
    }

    /**
     * Determines if a value is not within the specified list of values. If the array of arguments itself is {@code null}
     * then the argument is treated as {@code Object[1]} where that single value is {@code null}.
     *
     * @since 3.0.0-incubating
     */
    public static <V> P<V> without(final V... values) {
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
     * Determines if a value is not within the results of a child traversal resolved at runtime.
     *
     * @since 4.0.0
     */
    public static <V> P<V> without(final Traversal<?, ?> traversalValue) {
        return new P(Contains.without, Collections.singletonList(traversalValue.asAdmin()));
    }

    /**
     * Determines if a value is not within the results of child traversals resolved at runtime.
     * Each traversal is evaluated independently and results are combined into a single collection.
     *
     * @since 4.0.0
     */
    public static <V> P<V> without(final Traversal<?, ?> first, final Traversal<?, ?>... more) {
        final List<Traversal.Admin<?, ?>> admins = new ArrayList<>(1 + more.length);
        admins.add(first.asAdmin());
        for (final Traversal<?, ?> tv : more) {
            admins.add(tv.asAdmin());
        }
        return new P(Contains.without, admins);
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
}
