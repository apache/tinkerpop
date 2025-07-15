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

import org.apache.tinkerpop.gremlin.util.GremlinValueComparator;

/**
 * {@code Compare} is a {@code BiPredicate} that determines whether the first argument is {@code ==}, {@code !=},
 * {@code >}, {@code >=}, {@code <}, {@code <=} to the second argument.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Matt Frantz (http://github.com/mhfrantz)
 * @author Daniel Kuppitz (http://gemlin.guru)
 */
public enum Compare implements PBiPredicate<Object, Object> {

    /**
     * Evaluates if the first object is equal to the second per Gremlin Comparison semantics.
     *
     * @since 3.0.0-incubating
     */
    eq {
        @Override
        public boolean test(final Object first, final Object second) {
            return GremlinValueComparator.COMPARABILITY.equals(first, second);
        }
    },

    /**
     * Evaluates if the first object is not equal to the second per Gremlin Comparison semantics.
     *
     * @since 3.0.0-incubating
     */
    neq {
        @Override
        public boolean test(final Object first, final Object second) {
            return !eq.test(first, second);
        }
    },

    /**
     * Evaluates if the first object is greater than the second per Gremlin Comparison semantics.
     *
     * @since 3.0.0-incubating
     */
    gt {
        @Override
        public boolean test(final Object first, final Object second) {
            if (!GremlinValueComparator.COMPARABILITY.comparable(first, second)) {
                return false;
            }
            return GremlinValueComparator.COMPARABILITY.compare(first, second) > 0;
        }
    },

    /**
     * Evaluates if the first object is greater-equal to the second per Gremlin Comparison semantics.
     *
     * @since 3.0.0-incubating
     */
    gte {
        @Override
        public boolean test(final Object first, final Object second) {
            if (!GremlinValueComparator.COMPARABILITY.comparable(first, second)) {
                return false;
            }
            return GremlinValueComparator.COMPARABILITY.compare(first, second) >= 0;
        }
    },

    /**
     * Evaluates if the first object is less than the second per Gremlin Comparison semantics.
     *
     * @since 3.0.0-incubating
     */
    lt {
        @Override
        public boolean test(final Object first, final Object second) {
            if (!GremlinValueComparator.COMPARABILITY.comparable(first, second)) {
                return false;
            }
            return GremlinValueComparator.COMPARABILITY.compare(first, second) < 0;
        }
    },

    /**
     * Evaluates if the first object is less-equal to the second per Gremlin Comparison semantics.
     *
     * @since 3.0.0-incubating
     */
    lte {
        @Override
        public boolean test(final Object first, final Object second) {
            if (!GremlinValueComparator.COMPARABILITY.comparable(first, second)) {
                return false;
            }
            return GremlinValueComparator.COMPARABILITY.compare(first, second) <= 0;
        }
    };

}
