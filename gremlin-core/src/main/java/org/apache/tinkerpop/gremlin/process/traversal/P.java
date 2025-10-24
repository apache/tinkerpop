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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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

    public P(final PBiPredicate<V, V> biPredicate, final V value) {
        setValue(value);
        this.biPredicate = biPredicate;
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
        this.literals = new ArrayList<>(literals);
        this.isCollection = isCollection;
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
            Collection<V> values = this.literals.stream().collect(Collectors.toList());
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
                this.literals = new ArrayList<>();
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
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof P &&
                ((P) other).getClass().equals(this.getClass()) &&
                ((P) other).getBiPredicate().equals(this.biPredicate) &&
                ((((P) other).variables == null && this.variables == null) || (((P) other).variables != null && ((P) other).variables.equals(this.variables))) &&
                ((((P) other).literals == null && this.literals == null) || (((P) other).literals != null && CollectionUtils.isEqualCollection(((P) other).literals, this.literals)));
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
            return (P<V>) super.clone();
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
     * Determines if a value is of a type denoted by {@code GType}.
     *
     * @since 3.8.0
     */
    public static <V> P<V> typeOf(final GType value) {
        return new P(CompareType.typeOf, value.getType());
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
