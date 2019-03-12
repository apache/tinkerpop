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
package org.apache.tinkerpop.machine.functions.map;

import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.coefficients.Coefficient;
import org.apache.tinkerpop.machine.functions.AbstractFunction;
import org.apache.tinkerpop.machine.functions.MapFunction;
import org.apache.tinkerpop.machine.functions.NestedFunction;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.util.StringFactory;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapMap<C, S, E> extends AbstractFunction<C, S, E> implements MapFunction<C, S, E>, NestedFunction<C> {

    private final Compilation<C, S, E> internalMap;

    public MapMap(final Coefficient<C> coefficient, final Set<String> labels, final Compilation<C, S, E> internalMap) {
        super(coefficient, labels);
        this.internalMap = internalMap;
    }

    @Override
    public E apply(final Traverser<C, S> traverser) {
        return this.internalMap.mapTraverser(traverser).object();
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.internalMap);
    }
}
