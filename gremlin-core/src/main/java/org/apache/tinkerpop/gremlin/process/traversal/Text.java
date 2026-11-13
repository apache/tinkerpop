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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;

import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.Matcher; 

/**
 * {@link Text} is a {@code BiPredicate} that determines whether the first string starts with, starts
 * not with, ends with, ends not with, contains or does not contain the second string argument.
 *
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @since 3.4.0
 */
public enum Text implements PBiPredicate<String, String> {

    /**
     * Evaluates if the first string starts with the second.
     *
     * @since 3.4.0
     */
    startingWith {
        @Override
        public boolean test(final String value, final String prefix) {
            if (isNull(value, prefix)) return false;
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
            if (isNull(value, suffix)) return false;
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
            if (isNull(value, search)) return false;
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

    private static boolean isNull(final String... args) {
        for (String arg : args)
            if (arg == null)
                return true;
        return false;
    }

    /**
     * Produce the opposite representation of the current {@code Text} enum.
     */
    @Override
    public abstract Text negate();

    /**
     * Allows for a compiled version of the regex pattern.
     */
    public static class RegexPredicate implements PBiPredicate<String, String>, Serializable {

        private final Pattern pattern;
        private final boolean negate;

        public RegexPredicate(final String expression, final boolean negate) {
            pattern = Pattern.compile(expression);
            this.negate = negate;
        }

        public boolean isNegate() {
            return negate;
        }

        public String getPattern() {
            return pattern.pattern();
        }

        @Override
        public boolean test(final String value, final String expression) {
            // expression is unused - the pattern was compiled from it in the constructor.
            // The input is wrapped so an in-progress match can be interrupted (e.g. by an evaluationTimeout).
            final Matcher matcher = pattern.matcher(new InterruptibleCharSequence(value));
            return negate != matcher.find();
        }

        @Override
        public PBiPredicate<String, String> negate() {
            return new RegexPredicate(pattern.pattern(), !negate);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final RegexPredicate that = (RegexPredicate) o;
            return negate == that.negate && pattern.pattern().equals(that.pattern.pattern());
        }

        @Override
        public int hashCode() {
            return Objects.hash(pattern.pattern(), negate);
        }

        @Override
        public String toString() {
            return String.format("regex(%s)", pattern.pattern());
        }

        @Override
        public String getPredicateName() {
            return isNegate() ? "notRegex" : "regex";
        }
    }

    /**
     * A {@link CharSequence} that makes regex matching interruptible. A {@link Matcher} reads its input through
     * {@link #charAt(int)}, so checking the current thread's interrupt flag there lets an interrupt (such as an
     * {@code evaluationTimeout}) stop an in-progress match with a {@link TraversalInterruptedException}. As in
     * {@code AbstractStep}, the flag is cleared when the exception is thrown. The flag is checked once every
     * {@code CHECK_INTERVAL} reads to keep the per-character cost low.
     */
    private static final class InterruptibleCharSequence implements CharSequence {

        // Power of two so the periodic check is a cheap bitmask.
        private static final int CHECK_INTERVAL = 1024;

        private final CharSequence inner;
        private int calls;

        private InterruptibleCharSequence(final CharSequence inner) {
            this.inner = inner;
        }

        @Override
        public char charAt(final int index) {
            if ((++calls & (CHECK_INTERVAL - 1)) == 0 && Thread.interrupted())
                throw new TraversalInterruptedException();
            return inner.charAt(index);
        }

        @Override
        public int length() {
            return inner.length();
        }

        @Override
        public CharSequence subSequence(final int start, final int end) {
            return new InterruptibleCharSequence(inner.subSequence(start, end));
        }

        @Override
        public String toString() {
            return inner.toString();
        }
    }
}
