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

import org.apache.tinkerpop.gremlin.util.NumberHelper;

import java.util.function.BiPredicate;

/**
 * {@link Compare} is a {@link java.util.function.BiPredicate} that determines whether the first argument is {@code ==}, {@code !=},
 * {@code >}, {@code >=}, {@code <}, {@code <=} to the second argument.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Matt Frantz (http://github.com/mhfrantz)
 * @author Daniel Kuppitz (http://gemlin.guru)
 */
public enum Compare implements BiPredicate<Object, Object> {

    /**
     * Evaluates if the first object is equal to the second. If both are of type {@link Number}, {@link NumberHelper}
     * will be used for the comparison, thus enabling the comparison of only values, ignoring the number types.
     *
     * @since 3.0.0-incubating
     */
    eq {
        @Override
        public boolean test(final Object first, final Object second) {
            return null == first ? null == second : (bothAreNumber(first, second)
                    ? NumberHelper.compare((Number) first, (Number) second) == 0
                    : first.equals(second));
        }

        /**
         * The negative of {@code eq} is {@link #neq}.
         */
        @Override
        public Compare negate() {
            return neq;
        }
    },

    /**
     * Evaluates if the first object is not equal to the second. If both are of type {@link Number}, {@link NumberHelper}
     * will be used for the comparison, thus enabling the comparison of only values, ignoring the number types.
     *
     * @since 3.0.0-incubating
     */
    neq {
        @Override
        public boolean test(final Object first, final Object second) {
            return !eq.test(first, second);
        }

        /**
         * The negative of {@code neq} is {@link #eq}
         */
        @Override
        public Compare negate() {
            return eq;
        }
    },

    /**
     * Evaluates if the first object is greater than the second. If both are of type {@link Number}, {@link NumberHelper}
     * will be used for the comparison, thus enabling the comparison of only values, ignoring the number types.
     *
     * @since 3.0.0-incubating
     */
    gt {
        @Override
        public boolean test(final Object first, final Object second) {
            if (null != first && null != second) {
                if (bothAreNumber(first, second)) {
                    return NumberHelper.compare((Number) first, (Number) second) > 0;
                }
                if (bothAreComparableAndOfSameType(first, second)) {
                    return ((Comparable) first).compareTo(second) > 0;
                }
                throwException(first, second);
            }

            return false;
        }

        /**
         * The negative of {@code gt} is {@link #lte}.
         */
        @Override
        public Compare negate() {
            return lte;
        }
    },

    /**
     * Evaluates if the first object is greater-equal to the second. If both are of type {@link Number}, {@link NumberHelper}
     * will be used for the comparison, thus enabling the comparison of only values, ignoring the number types.
     *
     * @since 3.0.0-incubating
     */
    gte {
        @Override
        public boolean test(final Object first, final Object second) {
            return null == first ? null == second : (null != second && !lt.test(first, second));
        }

        /**
         * The negative of {@code gte} is {@link #lt}.
         */
        @Override
        public Compare negate() {
            return lt;
        }
    },

    /**
     * Evaluates if the first object is less than the second. If both are of type {@link Number}, {@link NumberHelper}
     * will be used for the comparison, thus enabling the comparison of only values, ignoring the number types.
     *
     * @since 3.0.0-incubating
     */
    lt {
        @Override
        public boolean test(final Object first, final Object second) {
            if (null != first && null != second) {
                if (bothAreNumber(first, second)) {
                    return NumberHelper.compare((Number) first, (Number) second) < 0;
                }
                if (bothAreComparableAndOfSameType(first, second)) {
                    return ((Comparable) first).compareTo(second) < 0;
                }
                throwException(first, second);
            }
            return false;
        }

        /**
         * The negative of {@code lt} is {@link #gte}.
         */
        @Override
        public Compare negate() {
            return gte;
        }
    },

    /**
     * Evaluates if the first object is less-equal to the second. If both are of type {@link Number}, {@link NumberHelper}
     * will be used for the comparison, thus enabling the comparison of only values, ignoring the number types.
     *
     * @since 3.0.0-incubating
     */
    lte {
        @Override
        public boolean test(final Object first, final Object second) {
            return null == first ? null == second : (null != second && !gt.test(first, second));
        }

        /**
         * The negative of {@code lte} is {@link #gt}.
         */
        @Override
        public Compare negate() {
            return gt;
        }
    };

    private static boolean bothAreNumber(Object first, Object second) {
        return first instanceof Number && second instanceof Number;
    }

    private static boolean bothAreComparableAndOfSameType(Object first, Object second) {
        return first instanceof Comparable && second instanceof Comparable
                && (first.getClass().isInstance(second) || second.getClass().isInstance(first));
    }

    private static void throwException(Object first, Object second) {
        throw new IllegalArgumentException(String.format("Cannot compare '%s' (%s) and '%s' (%s) as both need to be an instance of Number or Comparable (and of the same type)", first, first.getClass().getSimpleName(), second, second.getClass().getSimpleName()));
    }

    /**
     * Produce the opposite representation of the current {@code Compare} enum.
     */
    @Override
    public abstract Compare negate();
}
