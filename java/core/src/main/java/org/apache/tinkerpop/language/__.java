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
package org.apache.tinkerpop.language;

import org.apache.tinkerpop.machine.bytecode.Bytecode;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class __ {

    private __() {
        // static class
    }

    private static <C, S> Traversal<C, S, S> start() {
        return new Traversal<>(new Bytecode<>());
    }

    public static <C, S> Traversal<C, S, S> c(final C coefficient) {
        return __.<C, S>start().c(coefficient);
    }

    public static <C> Traversal<C, Long, Long> incr() {
        return __.<C, Long>start().incr();
    }

    public static <C, S extends Number> Traversal<C, S, S> sum() {
        return __.<C, S>start().sum();
    }

    public static <C, S> Traversal<C, S, Long> count() {
        return __.<C, S>start().count();
    }
}
