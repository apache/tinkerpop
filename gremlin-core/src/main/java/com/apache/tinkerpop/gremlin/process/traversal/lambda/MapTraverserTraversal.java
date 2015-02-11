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
package com.apache.tinkerpop.gremlin.process.traversal.lambda;

import com.apache.tinkerpop.gremlin.process.Traverser;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MapTraverserTraversal<S, E> extends AbstractLambdaTraversal<S, E> {

    private E e;
    private final Function<Traverser<S>, E> function;

    public MapTraverserTraversal(final Function<Traverser<S>, E> function) {
        this.function = function;
    }

    @Override
    public E next() {
        return this.e;
    }

    @Override
    public void addStart(final Traverser<S> start) {
        this.e = this.function.apply(start);
    }

    @Override
    public String toString() {
        return this.function.toString();
    }
}