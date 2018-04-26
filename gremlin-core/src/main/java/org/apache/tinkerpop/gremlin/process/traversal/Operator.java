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

import java.util.Collection;
import java.util.Map;
import java.util.function.BinaryOperator;

/**
 * A set of {@link BinaryOperator} instances that handle common operations for traversal steps.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Operator implements BinaryOperator<Object> {

    /**
     * An addition function.
     *
     * @since 3.0.0-incubating
     */
    sum {
        public Object apply(final Object a, Object b) {
            return NumberHelper.add((Number) a, (Number) b);
        }
    },

    /**
     * A subtraction function.
     *
     * @since 3.0.0-incubating
     */
    minus {
        public Object apply(final Object a, final Object b) {
            return NumberHelper.sub((Number) a, (Number) b);
        }
    },

    /**
     * A multiplication function.
     *
     * @since 3.0.0-incubating
     */
    mult {
        public Object apply(final Object a, final Object b) {
            return NumberHelper.mul((Number) a, (Number) b);
        }
    },

    /**
     * A division function.
     *
     * @since 3.0.0-incubating
     */
    div {
        public Object apply(final Object a, final Object b) {
            return NumberHelper.div((Number) a, (Number) b);
        }
    },

    /**
     * Selects the smaller of the values.
     *
     * @since 3.0.0-incubating
     */
    min {
        public Object apply(final Object a, final Object b) {
            return NumberHelper.min((Comparable) a, (Comparable) b);
        }
    },

    /**
     * Selects the larger of the values.
     *
     * @since 3.0.0-incubating
     */
    max {
        public Object apply(final Object a, final Object b) {
            return NumberHelper.max((Comparable) a, (Comparable) b);
        }
    },

    /**
     * The new incoming value (i.e. the second value to the function) is returned unchanged result in the assignment
     * of that value to the object of the {@code Operator}.
     *
     * @since 3.1.0-incubating
     */
    assign {
        public Object apply(final Object a, final Object b) {
            return b;
        }
    },

    /**
     * Applies "and" to boolean values.
     *
     * @since 3.2.0-incubating
     */
    and {
        public Object apply(final Object a, final Object b) {
            return ((boolean) a) && ((boolean) b);
        }
    },

    /**
     * Applies "or" to boolean values.
     *
     * @since 3.2.0-incubating
     */
    or {
        public Object apply(final Object a, final Object b) {
            return ((boolean) a) || ((boolean) b);
        }
    },

    /**
     * Takes all objects in the second {@code Collection} and adds them to the first.
     *
     * @since 3.2.0-incubating
     */
    addAll {
        public Object apply(final Object a, final Object b) {
            if (a instanceof Map)
                ((Map<?,?>) a).putAll((Map) b);
            else
                ((Collection<?>) a).addAll((Collection) b);
            return a;
        }
    },

    /**
     * Sums and adds long values.
     *
     * @since 3.2.0-incubating
     */
    sumLong {
        public Object apply(final Object a, final Object b) {
            return (long) a + (long) b;
        }
    }
}
