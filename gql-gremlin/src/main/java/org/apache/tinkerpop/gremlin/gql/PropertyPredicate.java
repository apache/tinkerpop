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
package org.apache.tinkerpop.gremlin.gql;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * A single property equality predicate attached to a node pattern in a GQL MATCH clause.
 *
 * <p>A predicate is either:
 * <ul>
 *   <li><strong>Literal</strong> — the expected value is a compile-time constant
 *       (string, long, double, or boolean) parsed from the query string.</li>
 *   <li><strong>Parameter reference</strong> — the expected value is resolved at execution
 *       time from the params map passed to {@code match(String, Map)}, keyed by
 *       {@link #getParamName()}.</li>
 * </ul>
 *
 * <p>Use {@link #ofLiteral(String, Object)} and {@link #ofParam(String, String)} to construct
 * instances. Use {@link #test(Element, Map)} to evaluate the predicate during DFS execution.
 */
public final class PropertyPredicate {

    private final String key;
    private final Object literalValue;
    private final String paramName;

    private PropertyPredicate(final String key, final Object literalValue, final String paramName) {
        this.key = key;
        this.literalValue = literalValue;
        this.paramName = paramName;
    }

    /**
     * Creates a literal predicate: {@code element.property(key) == value}.
     *
     * @param key   the property key
     * @param value the expected literal value (String, Long, Double, or Boolean)
     */
    public static PropertyPredicate ofLiteral(final String key, final Object value) {
        return new PropertyPredicate(key, value, null);
    }

    /**
     * Creates a parameter-reference predicate: the expected value is resolved from the
     * params map at execution time using {@code paramName} as the key.
     *
     * @param key       the property key
     * @param paramName the name of the parameter in the params map (without the {@code $} prefix)
     */
    public static PropertyPredicate ofParam(final String key, final String paramName) {
        return new PropertyPredicate(key, null, paramName);
    }

    /**
     * Evaluates this predicate against the given element.
     *
     * <p>The expected value is the literal (if this is a literal predicate) or the value
     * looked up from {@code params} by {@link #getParamName()} (if this is a param predicate).
     * A missing param key or a missing property both evaluate to {@code null}; the predicate
     * passes only if both sides are equal via {@link Objects#equals}.
     *
     * @param element the graph element to test
     * @param params  the parameter bindings from {@code match(String, Map)}; may be empty
     * @return {@code true} if any of the element's values for the key equal the expected value,
     *         or if the property is absent and the expected value is {@code null}
     */
    public boolean test(final Element element, final Map<String, Object> params) {
        final Object expected = paramName != null ? params.get(paramName) : literalValue;
        final Iterator<? extends Property<?>> props = element.properties(key);
        if (!props.hasNext()) return expected == null;
        while (props.hasNext()) {
            if (numericAwareEquals(props.next().value(), expected)) return true;
        }
        return false;
    }

    static boolean numericAwareEquals(final Object a, final Object b) {
        if (Objects.equals(a, b)) return true;
        if (!(a instanceof Number) || !(b instanceof Number)) return false;
        if (isNaN(a) && isNaN(b)) return true;
        if (isNaN(a) || isNaN(b)) return false;
        if (isInfinite(a) && isInfinite(b)) return ((Number) a).doubleValue() == ((Number) b).doubleValue();
        if (isInfinite(a) || isInfinite(b)) return false;
        try {
            return toComparableBigDecimal((Number) a).compareTo(toComparableBigDecimal((Number) b)) == 0;
        } catch (final NumberFormatException | ArithmeticException e) {
            return false;
        }
    }

    private static boolean isNaN(final Object v) {
        return (v instanceof Double && ((Double) v).isNaN()) || (v instanceof Float && ((Float) v).isNaN());
    }

    private static boolean isInfinite(final Object v) {
        return (v instanceof Double && ((Double) v).isInfinite()) || (v instanceof Float && ((Float) v).isInfinite());
    }

    private static BigDecimal toComparableBigDecimal(final Number n) {
        if (n instanceof BigDecimal) return (BigDecimal) n;
        if (n instanceof BigInteger) return new BigDecimal((BigInteger) n);
        if (n instanceof Double || n instanceof Float) return new BigDecimal(n.doubleValue());
        return BigDecimal.valueOf(n.longValue());
    }

    /** Returns the property key this predicate applies to. */
    public String getKey() {
        return key;
    }

    /**
     * Returns the literal expected value, or {@code null} if this is a parameter reference.
     */
    public Object getLiteralValue() {
        return literalValue;
    }

    /**
     * Returns the parameter name (without {@code $} prefix), or {@code null} if this is
     * a literal predicate.
     */
    public String getParamName() {
        return paramName;
    }

    /** Returns {@code true} if this predicate resolves its value from the params map. */
    public boolean isParamRef() {
        return paramName != null;
    }

    @Override
    public String toString() {
        if (paramName != null) {
            return key + " = $" + paramName;
        }
        return key + " = " + (literalValue instanceof String ? "'" + literalValue + "'" : literalValue);
    }
}
