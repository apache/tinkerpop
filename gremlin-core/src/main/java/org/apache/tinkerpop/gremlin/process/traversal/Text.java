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
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public enum Text implements BiPredicate<String, String> {

    startsWith {
        @Override
        public boolean test(final String value, final String prefix) {
            return value.startsWith(prefix);
        }

        /**
         * The negative of {@code startsWith} is {@link #startsNotWith}.
         */
        @Override
        public Text negate() {
            return startsNotWith;
        }
    },

    startsNotWith {
        @Override
        public boolean test(final String value, final String prefix) {
            return !startsWith.test(value, prefix);
        }

        /**
         * The negative of {@code startsNotWith} is {@link #startsWith}.
         */
        @Override
        public Text negate() {
            return startsWith;
        }
    },

    endsWith {
        @Override
        public boolean test(final String value, final String suffix) {
            return value.endsWith(suffix);
        }

        /**
         * The negative of {@code endsWith} is {@link #endsNotWith}.
         */
        @Override
        public Text negate() {
            return endsNotWith;
        }
    },

    endsNotWith {
        @Override
        public boolean test(final String value, final String prefix) {
            return !endsWith.test(value, prefix);
        }

        /**
         * The negative of {@code endsNotWith} is {@link #endsWith}.
         */
        @Override
        public Text negate() {
            return endsWith;
        }
    },

    contains {
        @Override
        public boolean test(final String value, final String search) {
            return value.contains(search);
        }

        /**
         * The negative of {@code contains} is {@link #absent}.
         */
        @Override
        public Text negate() {
            return absent;
        }
    },

    absent{
        @Override
        public boolean test(final String value, final String search) {
            return !contains.test(value, search);
        }

        /**
         * The negative of {@code absent} is {@link #contains}.
         */
        @Override
        public Text negate() {
            return contains;
        }
    };

    /**
     * Produce the opposite representation of the current {@code Text} enum.
     */
    @Override
    public abstract Text negate();
}