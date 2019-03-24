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
package org.apache.tinkerpop.language.gremlin.core;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class __ {

    private __() {
        // static class
    }

    private static <C, S> CoreTraversal<C, S, S> start() {
        return new CoreTraversal<>();
    }

    public static <C, S> CoreTraversal<C, S, S> c(final C coefficient) {
        return __.<C, S>start().c(coefficient);
    }

    public static <C, S> CoreTraversal<C, ?, S> constant(final S constant) {
        return __.<C, S>start().constant(constant);
    }

    public static <C> CoreTraversal<C, Long, Long> incr() {
        return __.<C, Long>start().incr();
    }

    public static <C, S> CoreTraversal<C, S, S> is(final S object) {
        return object instanceof CoreTraversal ? __.is((CoreTraversal<C, S, S>) object) : __.<C, S>start().is(object); // TODO: this is sucky. we don't want this
    }

    public static <C, S> CoreTraversal<C, S, S> is(final CoreTraversal<C, S, S> objectTraversal) {
        return __.<C, S>start().is(objectTraversal);
    }

    public static <C, S> CoreTraversal<C, S, Integer> loops() {
        return __.<C, S>start().loops();
    }

    public static <C, S extends Number> CoreTraversal<C, S, S> sum() {
        return __.<C, S>start().sum();
    }

    public static <C, S> CoreTraversal<C, S, Long> count() {
        return __.<C, S>start().count();
    }
}
