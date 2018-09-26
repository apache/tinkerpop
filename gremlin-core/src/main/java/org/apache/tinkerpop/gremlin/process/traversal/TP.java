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
public class TP extends P<String> {

    @SuppressWarnings("WeakerAccess")
    public TP(final BiPredicate<String, String> biPredicate, final String value) {
        super(biPredicate, value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TP && super.equals(other);
    }

    @Override
    public String toString() {
        return null == this.originalValue ? this.biPredicate.toString() : this.biPredicate.toString() + "(" + this.originalValue + ")";
    }

    @Override
    public TP negate() {
        return new TP(this.biPredicate.negate(), this.originalValue);
    }

    public TP clone() {
        return (TP) super.clone();
    }

    //////////////// statics

    /**
     * Determines if String does start with the given value.
     *
     * @since 3.4.0
     */
    public static TP startsWith(final String value) {
        return new TP(Text.startsWith, value);
    }

    /**
     * Determines if String does not start with the given value.
     *
     * @since 3.4.0
     */
    public static TP startsNotWith(final String value) {
        return new TP(Text.startsNotWith, value);
    }

    /**
     * Determines if String does start with the given value.
     *
     * @since 3.4.0
     */
    public static TP endsWith(final String value) {
        return new TP(Text.endsWith, value);
    }

    /**
     * Determines if String does not start with the given value.
     *
     * @since 3.4.0
     */
    public static TP endsNotWith(final String value) {
        return new TP(Text.endsNotWith, value);
    }

    /**
     * Determines if String does contain the given value.
     *
     * @since 3.4.0
     */
    public static TP contains(final String value) {
        return new TP(Text.contains, value);
    }

    /**
     * Determines if String does not contain the given value.
     *
     * @since 3.4.0
     */
    public static TP absent(final String value) {
        return new TP(Text.absent, value);
    }
}