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

import java.util.Comparator;
import java.util.Random;

import org.apache.tinkerpop.gremlin.util.NumberHelper;

/**
 * Provides {@code Comparator} instances for ordering traversers.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public enum Order implements Comparator<Object> {

    /**
     * Order in ascending fashion
     *
     * @since 3.0.0-incubating
     * @deprecated As of release 3.3.4, replaced by {@link #asc}.
     */
    incr {
        @Override
        public int compare(final Object first, final Object second) {
            return first instanceof Number && second instanceof Number
                    ? NumberHelper.compare((Number) first, (Number) second)
                    : Comparator.<Comparable>naturalOrder().compare((Comparable) first, (Comparable) second);
        }

        @Override
        public Order reversed() {
            return decr;
        }
    },

    /**
     * Order in descending fashion.
     *
     * @since 3.0.0-incubating
     * @deprecated As of release 3.3.4, replaced by {@link #desc}.
     */
    decr {
        @Override
        public int compare(final Object first, final Object second) {
            return first instanceof Number && second instanceof Number
                    ? NumberHelper.compare((Number) second, (Number) first)
                    : Comparator.<Comparable>reverseOrder().compare((Comparable) first, (Comparable) second);
        }

        @Override
        public Order reversed() {
            return incr;
        }
    },

    /**
     * Order in a random fashion.
     *
     * @since 3.0.0-incubating
     */
    shuffle {
        @Override
        public int compare(final Object first, final Object second) {
            return RANDOM.nextBoolean() ? -1 : 1;
        }

        @Override
        public Order reversed() {
            return shuffle;
        }
    },

    /**
     * Order in ascending fashion
     *
     * @since 3.3.4
     */
    asc {
        @Override
        public int compare(final Object first, final Object second) {
            return first instanceof Number && second instanceof Number
                    ? NumberHelper.compare((Number) first, (Number) second)
                    : Comparator.<Comparable>naturalOrder().compare((Comparable) first, (Comparable) second);
        }

        @Override
        public Order reversed() {
            return decr;
        }
    },

    /**
     * Order in descending fashion.
     *
     * @since 3.3.4
     */
    desc {
        @Override
        public int compare(final Object first, final Object second) {
            return first instanceof Number && second instanceof Number
                    ? NumberHelper.compare((Number) second, (Number) first)
                    : Comparator.<Comparable>reverseOrder().compare((Comparable) first, (Comparable) second);
        }

        @Override
        public Order reversed() {
            return incr;
        }
    };

    private static final Random RANDOM = new Random();

    /**
     * {@inheritDoc}
     */
    public abstract int compare(final Object first, final Object second);

    /**
     * Produce the opposite representation of the current {@code Order} enum.
     */
    @Override
    public abstract Order reversed();
}
