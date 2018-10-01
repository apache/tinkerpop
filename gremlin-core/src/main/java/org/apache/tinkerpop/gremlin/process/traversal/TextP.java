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
public class TextP extends P<String> {

    @SuppressWarnings("WeakerAccess")
    public TextP(final BiPredicate<String, String> biPredicate, final String value) {
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
    public static TextP startsWith(final String value) {
        return new TextP(Text.startsWith, value);
    }

    /**
     * Determines if String does not start with the given value.
     *
     * @since 3.4.0
     */
    public static TextP startsNotWith(final String value) {
        return new TextP(Text.startsNotWith, value);
    }

    /**
     * Determines if String does start with the given value.
     *
     * @since 3.4.0
     */
    public static TextP endsWith(final String value) {
        return new TextP(Text.endsWith, value);
    }

    /**
     * Determines if String does not start with the given value.
     *
     * @since 3.4.0
     */
    public static TextP endsNotWith(final String value) {
        return new TextP(Text.endsNotWith, value);
    }

    /**
     * Determines if String does contain the given value.
     *
     * @since 3.4.0
     */
    public static TextP contains(final String value) {
        return new TextP(Text.contains, value);
    }

    /**
     * Determines if String does not contain the given value.
     *
     * @since 3.4.0
     */
    public static TextP absent(final String value) {
        return new TextP(Text.absent, value);
    }
}