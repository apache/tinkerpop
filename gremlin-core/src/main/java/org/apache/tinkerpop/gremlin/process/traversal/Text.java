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

import java.util.function.BiPredicate;

/**
 * {@link Text} is a {@link java.util.function.BiPredicate} that determines whether the first string starts with, starts
 * not with, ends with, ends not with, contains or does not contain the second string argument.
 *
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @since 3.4.0
 */
public enum Text implements BiPredicate<String, String> {

    /**
     * Evaluates if the first string starts with the second.
     *
     * @since 3.4.0
     */
    startingWith {
        @Override
        public boolean test(final String value, final String prefix) {
            checkNull(value, prefix);
            return value.startsWith(prefix);
        }

        /**
         * The negative of {@code startsWith} is {@link #notStartingWith}.
         */
        @Override
        public Text negate() {
            return notStartingWith;
        }
    },

    /**
     * Evaluates if the first string does not start with the second.
     *
     * @since 3.4.0
     */
    notStartingWith {
        @Override
        public boolean test(final String value, final String prefix) {
            return !startingWith.test(value, prefix);
        }

        /**
         * The negative of {@code startsNotWith} is {@link #startingWith}.
         */
        @Override
        public Text negate() {
            return startingWith;
        }
    },

    /**
     * Evaluates if the first string ends with the second.
     *
     * @since 3.4.0
     */
    endingWith {
        @Override
        public boolean test(final String value, final String suffix) {
            checkNull(value, suffix);
            return value.endsWith(suffix);
        }

        /**
         * The negative of {@code endsWith} is {@link #notEndingWith}.
         */
        @Override
        public Text negate() {
            return notEndingWith;
        }
    },

    /**
     * Evaluates if the first string does not end with the second.
     *
     * @since 3.4.0
     */
    notEndingWith {
        @Override
        public boolean test(final String value, final String prefix) {
            return !endingWith.test(value, prefix);
        }

        /**
         * The negative of {@code endsNotWith} is {@link #endingWith}.
         */
        @Override
        public Text negate() {
            return endingWith;
        }
    },

    /**
     * Evaluates if the first string contains the second.
     *
     * @since 3.4.0
     */
    containing {
        @Override
        public boolean test(final String value, final String search) {
            checkNull(value, search);
            return value.contains(search);
        }

        /**
         * The negative of {@code contains} is {@link #notContaining}.
         */
        @Override
        public Text negate() {
            return notContaining;
        }
    },

    /**
     * Evaluates if the first string does not contain the second.
     *
     * @since 3.4.0
     */
    notContaining {
        @Override
        public boolean test(final String value, final String search) {
            return !containing.test(value, search);
        }

        /**
         * The negative of {@code absent} is {@link #containing}.
         */
        @Override
        public Text negate() {
            return containing;
        }
    };

    private static final void checkNull(final String... args) {
        for (String arg : args)
            if (arg == null)
                throw new GremlinTypeErrorException();
    }

    /**
     * Produce the opposite representation of the current {@code Text} enum.
     */
    @Override
    public abstract Text negate();
}
