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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op;

import org.apache.tinkerpop.gremlin.structure.T;

import java.util.List;
import java.util.Objects;

/**
 * The forms of a {@code by()} modulator that compile natively: a key is evaluated per entry to the value that
 * {@code dedup}, {@code groupCount}, {@code group}, {@code order} and {@code choose} work on.
 */
public final class Keys {

    private Keys() {
    }

    /**
     * What a {@link Key} reads from the lane it is applied to.
     */
    public interface Key {

        /**
         * The nested plans of the key, non-empty only for {@link Child}.
         */
        default List<CsrPlan> children() {
            return List.of();
        }
    }

    /**
     * The entry itself: {@code by()} absent or {@code IdentityTraversal}.
     */
    public record Identity() implements Key {
    }

    /**
     * {@code TokenTraversal}: {@code T.id}, {@code T.label}, {@code T.key} or {@code T.value}.
     */
    public record Token(T token) implements Key {
        public Token {
            Objects.requireNonNull(token);
        }
    }

    /**
     * {@code ValueTraversal}: the property with the key code, read through the lane's element.
     *
     * @param keyCode    the vertex or edge key code, by the lane the key is applied to
     * @param name       the key name, for the exception a multi-property raises
     * @param productive false when a missing property makes the entry non-productive (filtered), true when it maps to
     *                   null ({@code ProductiveByStrategy}'s {@code coalesce(child, constant(null))})
     */
    public record Value(int keyCode, String name, boolean productive) implements Key {
        public Value {
            Objects.requireNonNull(name);
        }
    }

    /**
     * {@code ConstantTraversal} and {@code GValueConstantTraversal}, already reduced to the value.
     */
    public record Const(Object value) implements Key {
    }

    /**
     * A child traversal, compiled to a plan that starts with {@link Sources.Input} of the lane the key is applied to;
     * the key is the first result, and an empty result is non-productive.
     */
    public record Child(CsrPlan plan) implements Key {
        public Child {
            Objects.requireNonNull(plan);
        }

        @Override
        public List<CsrPlan> children() {
            return List.of(plan);
        }
    }
}
