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

/**
 * Predefined {@code Predicate} values that can be used as {@code String} filters.
 *
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class TextP extends P<String> {

    public TextP(final PBiPredicate<String, String> biPredicate, final String value) {
        super(biPredicate, value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TextP && super.equals(other);
    }

    @Override
    public String toString() {
        return null == this.originalValue ? this.biPredicate.toString() : this.biPredicate.toString() + "(" + this.originalValue + ")";
    }

    @Override
    public TextP negate() {
        return new TextP(this.biPredicate.negate(), this.originalValue);
    }

    public TextP clone() {
        return (TextP) super.clone();
    }

    //////////////// statics

    /**
     * Determines if String does start with the given value.
     *
     * @since 3.4.0
     */
    public static TextP startingWith(final String value) {
        return new TextP(Text.startingWith, value);
    }

    /**
     * Determines if String does not start with the given value.
     *
     * @since 3.4.0
     */
    public static TextP notStartingWith(final String value) {
        return new TextP(Text.notStartingWith, value);
    }

    /**
     * Determines if String does start with the given value.
     *
     * @since 3.4.0
     */
    public static TextP endingWith(final String value) {
        return new TextP(Text.endingWith, value);
    }

    /**
     * Determines if String does not start with the given value.
     *
     * @since 3.4.0
     */
    public static TextP notEndingWith(final String value) {
        return new TextP(Text.notEndingWith, value);
    }

    /**
     * Determines if String does contain the given value.
     *
     * @since 3.4.0
     */
    public static TextP containing(final String value) {
        return new TextP(Text.containing, value);
    }

    /**
     * Determines if String does not contain the given value.
     *
     * @since 3.4.0
     */
    public static TextP notContaining(final String value) {
        return new TextP(Text.notContaining, value);
    }
    
    /**           
     * Determines if String has a match with the given regex pattern. The TinkerPop reference implementation uses
     * Java syntax for regex. The string is considered a match to the pattern if any substring matches the pattern. It
     * is therefore important to use the appropriate boundary matchers (e.g. `$` for end of a line) to ensure a proper
     * match.
     *
     * @since 3.6.0
     */
    public static TextP regex(final String value) {
        return new TextP(new Text.RegexPredicate(value, false), value);
    }

    /**           
     * Determines if String has no match with the given regex pattern and the reference implementation treats it as a
     * simple negation of the evaluation of the pattern match of {@link #regex(String)}.
     *
     * @since 3.6.0
     */
    public static TextP notRegex(final String value) {
        return new TextP(new Text.RegexPredicate(value, true), value);
    }
}
