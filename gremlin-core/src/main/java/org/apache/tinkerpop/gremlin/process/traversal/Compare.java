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

import java.math.BigDecimal;
import java.util.function.BiPredicate;

/**
 * {@link Compare} is a {@link java.util.function.BiPredicate} that determines whether the first argument is {@code ==}, {@code !=},
 * {@code >}, {@code >=}, {@code <}, {@code <=} to the second argument.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Matt Frantz (http://github.com/mhfrantz)
 */
public enum Compare implements BiPredicate<Object, Object> {
    /**
     * Evaluates if the first object is equal to the second.  If both are of type {@link Number} but not of the
     * same class (i.e. double for the first object and long for the second object) both values are converted to
     * {@link BigDecimal} so that it can be evaluated via {@link BigDecimal#compareTo}.  Otherwise they are evaluated
     * via {@link Object#equals(Object)}.  Testing against {@link Number#doubleValue()} enables the compare
     * operations to be a bit more forgiving with respect to comparing different number types.
     *
     * @since 3.0.0-incubating
     */
    eq {
        @Override
        public boolean test(final Object first, final Object second) {
            return null == first ? null == second : (first instanceof Number && second instanceof Number
                    && !first.getClass().equals(second.getClass())
                    ? big((Number) first).compareTo(big((Number) second)) == 0
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
     * Evaluates if the first object is not equal to the second.  If both are of type {@link Number} but not of the
     * same class (i.e. double for the first object and long for the second object) both values are converted to
     * {@link BigDecimal} so that it can be evaluated via {@link BigDecimal#equals}.  Otherwise they are evaluated
     * via {@link Object#equals(Object)}.  Testing against {@link Number#doubleValue()} enables the compare
     * operations to be a bit more forgiving with respect to comparing different number types.
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
     * Evaluates if the first object is greater than the second.  If both are of type {@link Number} but not of the
     * same class (i.e. double for the first object and long for the second object) both values are converted to
     * {@link BigDecimal} so that it can be evaluated via {@link BigDecimal#compareTo}.  Otherwise they are evaluated
     * via {@link Comparable#compareTo(Object)}.  Testing against {@link BigDecimal#compareTo} enables the compare
     * operations to be a bit more forgiving with respect to comparing different number types.
     *
     * @since 3.0.0-incubating
     */
    gt {
        @Override
        public boolean test(final Object first, final Object second) {
            return null != first && null != second && (
                    first instanceof Number && second instanceof Number && !first.getClass().equals(second.getClass())
                            ? big((Number) first).compareTo(big((Number) second)) > 0
                            : ((Comparable) first).compareTo(second) > 0);
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
     * Evaluates if the first object is greater-equal to the second.  If both are of type {@link Number} but not of the
     * same class (i.e. double for the first object and long for the second object) both values are converted to
     * {@link BigDecimal} so that it can be evaluated via {@link BigDecimal#compareTo}.  Otherwise they are evaluated
     * via {@link Comparable#compareTo(Object)}.  Testing against {@link BigDecimal#compareTo} enables the compare
     * operations to be a bit more forgiving with respect to comparing different number types.
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
     * Evaluates if the first object is less than the second.  If both are of type {@link Number} but not of the
     * same class (i.e. double for the first object and long for the second object) both values are converted to
     * {@link BigDecimal} so that it can be evaluated via {@link BigDecimal#compareTo}.  Otherwise they are evaluated
     * via {@link Comparable#compareTo(Object)}.  Testing against {@link BigDecimal#compareTo} enables the compare
     * operations to be a bit more forgiving with respect to comparing different number types.
     *
     * @since 3.0.0-incubating
     */
    lt {
        @Override
        public boolean test(final Object first, final Object second) {
            return null != first && null != second && (
                    first instanceof Number && second instanceof Number && !first.getClass().equals(second.getClass())
                            ? big((Number) first).compareTo(big((Number) second)) < 0
                            : ((Comparable) first).compareTo(second) < 0);
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
     * Evaluates if the first object is less-equal to the second.  If both are of type {@link Number} but not of the
     * same class (i.e. double for the first object and long for the second object) both values are converted to
     * {@link BigDecimal} so that it can be evaluated via {@link BigDecimal#compareTo}.  Otherwise they are evaluated
     * via {@link Comparable#compareTo(Object)}.  Testing against {@link BigDecimal#compareTo} enables the compare
     * operations to be a bit more forgiving with respect to comparing different number types.
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

    /**
     * Produce the opposite representation of the current {@code Compare} enum.
     */
    @Override
    public abstract Compare negate();

    /**
     * Convert Number to BigDecimal.
     */
    private static BigDecimal big(final Number n) {
        return new BigDecimal(n.toString());
    }
}
