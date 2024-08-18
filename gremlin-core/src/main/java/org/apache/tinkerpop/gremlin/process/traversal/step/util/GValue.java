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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A {@code GValue} is a variable or literal value that is used in a {@link Traversal}. It is composed of a key-value
 * pair where the key is the name given to the variable and the value is the object that the variable resolved to. If
 * the name is not given, the value was provided literally in the traversal. The value of the variable can be any
 * object. The {@code GValue} also includes the {@link GType} that describes the type it contains.
 */
public class GValue<V> implements Cloneable, Serializable {
    private final String name;
    private final GType type;

    private final V value;

    private GValue(final GType type, final V value) {
        this(null, type, value);
    }

    private GValue(final String name, final GType type, final V value) {
        this.name = name;
        this.type = type;
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
     * Gets the name of the variable if it was defined as such and returns empty if the value was a literal.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Gets the type of the value. The explicit type could be determined with {@code instanceof} on the value, but this
     * might be helpful for cases where the value was constructed with a {@code null} value which might just return as
     * {@code Object}.
     */
    public GType getType() {
        return this.type;
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
                String.format("%s&%s", name, value) : Objects.toString(value);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GValue<?> gValue = (GValue<?>) o;
        return Objects.equals(name, gValue.name) && type == gValue.type && Objects.equals(value, gValue.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, value);
    }

    /**
     * Create a new {@code Var} from a particular value but without the specified name.
     *
     * @param value the value of the variable
     */
    public static <V> GValue<V> of(final V value) {
        if (value instanceof GValue)
            return (GValue<V>) value;

        return new GValue<>(GType.getType(value), value);
    }

    /**
     * Create a new {@code Var} with the specified name and value.
     *
     * @param name the name of the variable
     * @param value the value of the variable
     */
    public static <V> GValue<V> of(final String name, final V value) {
        return new GValue<>(name, GType.getType(value), value);
    }

    /**
     * Create a new {@code GValue} for a string value.
     */
    public static GValue<String> ofString(final String value) {
        return new GValue<>(GType.STRING, value);
    }

    /**
     * Create a new {@code GValue} for a string value with a specified name.
     */
    public static GValue<String> ofString(final String name, final String value) {
        return new GValue<>(name, GType.STRING, value);
    }

    /**
     * Create a new {@code GValue} for an integer value.
     */
    public static GValue<Integer> ofInteger(final Integer value) {
        return new GValue<>(GType.INTEGER, value);
    }

    /**
     * Create a new {@code GValue} for an integer value with a specified name.
     */
    public static GValue<Integer> ofInteger(final String name, final Integer value) {
        return new GValue<>(name, GType.INTEGER, value);
    }

    /**
     * Create a new {@code GValue} for a boolean value.
     */
    public static GValue<Boolean> ofBoolean(final Boolean value) {
        return new GValue<>(GType.BOOLEAN, value);
    }

    /**
     * Create a new {@code GValue} for a boolean value with a specified name.
     */
    public static GValue<Boolean> ofBoolean(final String name, final Boolean value) {
        return new GValue<>(name, GType.BOOLEAN, value);
    }

    /**
     * Create a new {@code GValue} for a double value.
     */
    public static GValue<Double> ofDouble(final Double value) {
        return new GValue<>(GType.DOUBLE, value);
    }

    /**
     * Create a new {@code GValue} for a double value with a specified name.
     */
    public static GValue<Double> ofDouble(final String name, final Double value) {
        return new GValue<>(name, GType.DOUBLE, value);
    }

    /**
     * Create a new {@code GValue} for a BigInteger value.
     */
    public static GValue<BigInteger> ofBigInteger(final BigInteger value) {
        return new GValue<>(GType.BIG_INTEGER, value);
    }

    /**
     * Create a new {@code GValue} for a BigInteger value with a specified name.
     */
    public static GValue<BigInteger> ofBigInteger(final String name, final BigInteger value) {
        return new GValue<>(name, GType.BIG_INTEGER, value);
    }

    /**
     * Create a new {@code GValue} for a BigDecimal value.
     */
    public static GValue<BigDecimal> ofBigDecimal(final BigDecimal value) {
        return new GValue<>(GType.BIG_DECIMAL, value);
    }

    /**
     * Create a new {@code GValue} for a BigDecimal value with a specified name.
     */
    public static GValue<BigDecimal> ofBigDecimal(final String name, final BigDecimal value) {
        return new GValue<>(name, GType.BIG_DECIMAL, value);
    }

    /**
     * Create a new {@code GValue} for a long value.
     */
    public static GValue<Long> ofLong(final Long value) {
        return new GValue<>(GType.LONG, value);
    }

    /**
     * Create a new {@code GValue} for a long value with a specified name.
     */
    public static GValue<Long> ofLong(final String name, final Long value) {
        return new GValue<>(name, GType.LONG, value);
    }

    /**
     * Create a new {@code GValue} for a map value.
     */
    public static GValue<Map> ofMap(final Map value) {
        return new GValue<>(GType.MAP, value);
    }

    /**
     * Create a new {@code GValue} for a map value with a specified name.
     */
    public static GValue<Map> ofMap(final String name, final Map value) {
        return new GValue<>(name, GType.MAP, value);
    }

    /**
     * Create a new {@code GValue} for a list value.
     */
    public static <T> GValue<List<T>> ofList(final List<T> value) {
        return new GValue<>(GType.LIST, value);
    }

    /**
     * Create a new {@code GValue} for a list value with a specified name.
     */
    public static <T> GValue<List<T>> ofList(final String name, final List<T> value) {
        return new GValue<>(name, GType.LIST, value);
    }

    /**
     * Create a new {@code GValue} for a set value.
     */
    public static GValue<Set> ofSet(final Set value) {
        return new GValue<>(GType.SET, value);
    }

    /**
     * Create a new {@code GValue} for a set value with a specified name.
     */
    public static GValue<Set> ofSet(final String name, final Set value) {
        return new GValue<>(name, GType.SET, value);
    }

    /**
     * Create a new {@code GValue} for a vertex value.
     */
    public static GValue<Vertex> ofVertex(final Vertex value) {
        return new GValue<>(GType.VERTEX, value);
    }

    /**
     * Create a new {@code GValue} for a vertex value with a specified name.
     */
    public static GValue<Vertex> ofVertex(final String name, final Vertex value) {
        return new GValue<>(name, GType.VERTEX, value);
    }

    /**
     * Create a new {@code GValue} for an edge value.
     */
    public static GValue<Edge> ofEdge(final Edge value) {
        return new GValue<>(GType.EDGE, value);
    }

    /**
     * Create a new {@code GValue} for an edge value with a specified name.
     */
    public static GValue<Edge> ofEdge(final String name, final Edge value) {
        return new GValue<>(name, GType.EDGE, value);
    }

    /**
     * Create a new {@code GValue} for a path value.
     */
    public static GValue<Path> ofPath(final Path value) {
        return new GValue<>(GType.PATH, value);
    }

    /**
     * Create a new {@code GValue} for a path value with a specified name.
     */
    public static GValue<Path> ofPath(final String name, final Path value) {
        return new GValue<>(name, GType.PATH, value);
    }

    /**
     * Create a new {@code GValue} for a property value.
     */
    public static GValue<Property> ofProperty(final Property value) {
        return new GValue<>(GType.PROPERTY, value);
    }

    /**
     * Create a new {@code GValue} for a property value with a specified name.
     */
    public static GValue<Property> ofProperty(final String name, final Property value) {
        return new GValue<>(name, GType.PROPERTY, value);
    }

    public static <T> GValue[] promoteGValues(final Object... objects) {
        return Arrays.stream(objects).map(o -> {
            if (o instanceof GValue)
                return (GValue<T>) o;
            else
                return GValue.of(o);
        }).toArray(GValue[]::new);
    }

    /**
     * The objects array is a series of key-value pairs. Construct a new object array that ensures that the values
     * become {@link GValue} instances if they are not already.
     */
    public static Object[] promoteGValuesInKeyValues(final Object... objects) {
        final Object[] keyValues = new Object[objects.length];
        for (int i = 0; i < objects.length; i++) {
            final Object o = objects[i];

            // even are keys, odd are values
            if (i % 2 == 1) {
                keyValues[i] = o instanceof GValue ? o : GValue.of(o);
            } else {
                keyValues[i] = o;
            }
        }
        return keyValues;
    }

    /**
     * Resolve all {@link GValue} instance in the array to their value and return a new object array.
     */
    public static Object[] resolveGValues(final Object... objects) {
        return Arrays.stream(objects).map(o -> {
            if (o instanceof GValue)
                return ((GValue) o).get();
            else
                return o;
        }).toArray();
    }

    /**
     * Take an instance of {@link P} and if it is a {@link GValue}, unwrap it to its value and reassign it back to
     * the {@link P} instance. In addition, Detect if the {@link P} instance is a {@link ConnectiveP}, then iterate
     * the list of predicates and convert them to {@link GValue} if they are instances of {@link GValue}. This must be
     * done recursively as the {@link ConnectiveP} can contain other {@link ConnectiveP} instances.
     */
    public static P resolveGValuesInP(final P p) {
        final Object o = p.getValue();
        if (o instanceof GValue) {
            if (o instanceof GValue) {
                p.setValue(((GValue) o).get());
            }
        } else if (o instanceof List) {
            final List l = (List) o;
            final List ln = new ArrayList<>();
            l.forEach(value -> ln.add(value instanceof GValue ? ((GValue) value).get() : value) );
            p.setValue(ln);
        } else if (p instanceof ConnectiveP) {
            final ConnectiveP connectiveP = (ConnectiveP) p;
            connectiveP.getPredicates().forEach(pred -> resolveGValuesInP((P) pred));
        }
        return p;
    }

    public static HasContainer resolveGValuesInHasContainer(final HasContainer hasContainer) {
        final P p = hasContainer.getPredicate();
        hasContainer.setPredicate(resolveGValuesInP(p));
        return hasContainer;
    }

    public static HasContainer[] resolveGValuesInHasContainers(final HasContainer... hasContainers) {
        for (int i = 0; i < hasContainers.length; i++) {
            hasContainers[i] = resolveGValuesInHasContainer(hasContainers[i]);
        }
        return hasContainers;
    }

    /**
     * Detects if a value of {@link P} has a {@link GValue} and if so, return {@link true} immediately.
     */
    public static boolean hasGValueInP(final P p) {
        final Object o = p.getValue();
        if (o instanceof GValue)
            return true;
        else if (o instanceof List) {
            final List l = (List) o;
            for (final Object value : l) {
                if (value instanceof GValue)
                    return true;
            }
        } else if (p instanceof ConnectiveP) {
            final ConnectiveP<?> connectiveP = (ConnectiveP) p;
            for (P pred : connectiveP.getPredicates()) {
                if (hasGValueInP(pred))
                    return true;
            }
        }
        return false;
    }

    public static boolean hasGValue(final Object... objects) {
        for (Object o : objects) {
            if (o instanceof GValue)
                return true;
        }
        return false;
    }
}