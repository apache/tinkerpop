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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.GValueConstantTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A {@code GValue} is a variable or literal value that is used in a {@link Traversal}. It is composed of a key-value
 * pair where the key is the name given to the variable and the value is the object that the variable resolved to. If
 * the name is not given, the value was provided literally in the traversal. The value of the variable can be any
 * object.
 */
public class GValue<V> implements Serializable {
    private final String name;
    private final V value;

    private GValue(final V value) {
        this(null, value);
    }

    private GValue(final String name, final V value) {
        if (name != null && name.startsWith("_")) {
            throw new IllegalArgumentException(String.format("Invalid GValue name [%s]. Should not start with _.", name));
        }
        if (value instanceof GValue) {
            throw new IllegalArgumentException("GValues cannot be nested");
        }
        this.name = name;
        this.value = value;
    }

    /**
     * Determines if the value held by this object was defined as a variable or a literal value. Literal values simply
     * have no name.
     */
    public boolean isVariable() {
        return this.name != null;
    }

    /**
     * Gets the name of the variable if it was defined as such and returns null if the value was a literal.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Determines if the value held is of a {@code null} value.
     */
    public boolean isNull() {
        return this.value == null;
    }

    /**
     * Gets the value.
     */
    public V get() {
        return this.value;
    }

    @Override
    public String toString() {
        return isVariable() ?
                String.format("%s=%s", name, value) : Objects.toString(value);
    }

    @Override
    public boolean equals(final Object o) { //TODO revisit equality and hash.
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GValue<?> gValue = (GValue<?>) o;
        return Objects.equals(name, gValue.name) && Objects.equals(value, gValue.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public GValue<?> clone() throws CloneNotSupportedException {
        return new GValue<>(this.name, this.value);
    }

    /**
     * Create a new {@code Var} from a particular value but without the specified name. If the argument provide is
     * already a {@code GValue} then it is returned as-is.
     *
     * @param value the value of the variable
     */
    public static <V> GValue<V> of(final V value) {
        if (value instanceof GValue) return (GValue) value;
        return new GValue<>(value);
    }

    /**
     * Create a new {@code Var} with the specified name and value. If the argument provide is already a
     * {@code GValue} then an IllegalArgumentException is thrown.
     *
     * @param name the name of the variable
     * @param value the value of the variable
     * @throws IllegalArgumentException if value is already a {@code GValue}
     */
    public static <V> GValue<V> of(final String name, final V value) {
        if (value instanceof GValue) throw new IllegalArgumentException("value cannot be a GValue");
        return new GValue<>(name, value);
    }

    /**
     * Create a new {@code GValue} for a string value.
     */
    public static GValue<String> ofString(final String value) {
        return new GValue<>(value);
    }

    /**
     * Create a new {@code GValue} for a string value with a specified name.
     */
    public static GValue<String> ofString(final String name, final String value) {
        return new GValue<>(name, value);
    }

    /**
     * Create a new {@code GValue} for an integer value.
     */
    public static GValue<Integer> ofInteger(final Integer value) {
        return new GValue<>(value);
    }

    /**
     * Create a new {@code GValue} for an integer value with a specified name.
     */
    public static GValue<Integer> ofInteger(final String name, final Integer value) {
        return new GValue<>(name, value);
    }

    /**
     * Create a new {@code GValue} for a boolean value.
     */
    public static GValue<Boolean> ofBoolean(final Boolean value) {
        return new GValue<>(value);
    }

    /**
     * Create a new {@code GValue} for a boolean value with a specified name.
     */
    public static GValue<Boolean> ofBoolean(final String name, final Boolean value) {
        return new GValue<>(name, value);
    }

    /**
     * Create a new {@code GValue} for a double value.
     */
    public static GValue<Double> ofDouble(final Double value) {
        return new GValue<>(value);
    }

    /**
     * Create a new {@code GValue} for a double value with a specified name.
     */
    public static GValue<Double> ofDouble(final String name, final Double value) {
        return new GValue<>(name, value);
    }

    /**
     * Create a new {@code GValue} for a BigInteger value.
     */
    public static GValue<BigInteger> ofBigInteger(final BigInteger value) {
        return new GValue<>(value);
    }

    /**
     * Create a new {@code GValue} for a BigInteger value with a specified name.
     */
    public static GValue<BigInteger> ofBigInteger(final String name, final BigInteger value) {
        return new GValue<>(name, value);
    }

    /**
     * Create a new {@code GValue} for a BigDecimal value.
     */
    public static GValue<BigDecimal> ofBigDecimal(final BigDecimal value) {
        return new GValue<>(value);
    }

    /**
     * Create a new {@code GValue} for a BigDecimal value with a specified name.
     */
    public static GValue<BigDecimal> ofBigDecimal(final String name, final BigDecimal value) {
        return new GValue<>(name, value);
    }

    /**
     * Create a new {@code GValue} for a long value.
     */
    public static GValue<Long> ofLong(final Long value) {
        return new GValue<>(value);
    }

    /**
     * Create a new {@code GValue} for a long value with a specified name.
     */
    public static GValue<Long> ofLong(final String name, final Long value) {
        return new GValue<>(name, value);
    }

    /**
     * Create a new {@code GValue} for a map value.
     */
    public static GValue<Map> ofMap(final Map value) {
        return new GValue<>(value);
    }

    /**
     * Create a new {@code GValue} for a map value with a specified name.
     */
    public static GValue<Map<?,?>> ofMap(final String name, final Map value) {
        return new GValue<>(name, value);
    }

    /**
     * Create a new {@code GValue} for a list value.
     */
    public static <T> GValue<List<T>> ofList(final List<T> value) {
        return new GValue<>(value);
    }

    /**
     * Create a new {@code GValue} for a list value with a specified name.
     */
    public static <T> GValue<List<T>> ofList(final String name, final List<T> value) {
        return new GValue<>(name, value);
    }

    /**
     * Create a new {@code GValue} for a set value.
     */
    public static GValue<Set> ofSet(final Set value) {
        return new GValue<>(value);
    }

    /**
     * Create a new {@code GValue} for a set value with a specified name.
     */
    public static GValue<Set> ofSet(final String name, final Set value) {
        return new GValue<>(name, value);
    }

    /**
     * Create a new {@code GValue} for a vertex value.
     */
    public static GValue<Vertex> ofVertex(final Vertex value) {
        return new GValue<>(value);
    }

    /**
     * Create a new {@code GValue} for a vertex value with a specified name.
     */
    public static GValue<Vertex> ofVertex(final String name, final Vertex value) {
        return new GValue<>(name, value);
    }

    /**
     * If the object is a {@code GValue} then get its value, otherwise return the object as-is.
     */
    public static <T> T getFrom(final Object o) {
        return o instanceof GValue ? ((GValue<T>) o).get() : (T) o;
    }

    /**
     * Tests if the object is a {@link GValue} and if so, checks the type of the value against the provided
     * {@link Class}.
     */
    public static boolean valueInstanceOf(final Object o, final Class type) {
        return o instanceof GValue && type.isAssignableFrom(((GValue<?>) o).get().getClass());
    }

    /**
     * The elements in object array argument are examined to see if they are {@link GValue} objects. If they are, they
     * are preserved as is. If they are not then they are wrapped in a {@link GValue} object.
     */
    public static <T> GValue<T>[] ensureGValues(final Object[] args) {
        return Stream.of(args).map(GValue::of).toArray(GValue[]::new);
    }

    /**
     * Converts {@link GValue} objects argument array to their values to prevent them from leaking to the Graph API.
     * Providers extending from this step should use this method to get actual values to prevent any {@link GValue}
     * objects from leaking to the Graph API.
     */
    public static Object[] resolveToValues(final GValue<?>[] gvalues) {
        final Object[] values = new Object[gvalues.length];
        for (int i = 0; i < gvalues.length; i++) {
            values[i] = gvalues[i].get();
        }
        return values;
    }

    /**
     * Converts possible {@link GValue} objects argument array to their values to prevent them from leaking to the Graph API.
     * Providers extending from this step should use this method to get actual values to prevent any {@link GValue}
     * objects from leaking to the Graph API.
     */
    public static Object[] resolveToValues(final Object[] gvalues) {
        final Object[] values = new Object[gvalues.length];
        for (int i = 0; i < gvalues.length; i++) {
            values[i] = gvalues[i] instanceof GValue ? ((GValue<?>) gvalues[i]).get() : gvalues[i];
            if (values[i] instanceof GValueConstantTraversal) {
                // TODO:: This seems out of place here, find a better home for this.
                values[i] = ((GValueConstantTraversal<?, ?>) values[i]).getConstantTraversal();
            }
        }
        return values;
    }

    /**
     * Tests if any of the objects are GValues.
     */
    public static boolean containsGValues(final Object... args) {
        for (Object arg : args) {
            if (arg instanceof GValue) {
                return true;
            }
        }
        return false;
    }

    /**
     * Tests if any of the objects are GValues or GValueConstantTraversal, and if so, if they are variables.
     */
    public static boolean containsVariables(final Object... args) {
        for (Object arg : args) {
            if (arg instanceof GValue && ((GValue<?>) arg).isVariable()) {
                return true;
            } else if (arg instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, ?>) arg).isParameterized()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Takes an argument that is either a {@code GValue} or an object and if the former, returns the child object and
     * if the latter returns the object itself.
     */
    public static Object valueOf(final Object either) {
        if (either instanceof GValue)
            return ((GValue<?>) either).get();
        else
            return either;
    }
}
