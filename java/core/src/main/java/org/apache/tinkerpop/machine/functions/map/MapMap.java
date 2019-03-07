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

import org.apache.tinkerpop.machine.Processor;
import org.apache.tinkerpop.machine.functions.AbstractFunction;
import org.apache.tinkerpop.machine.functions.CFunction;
import org.apache.tinkerpop.machine.functions.MapFunction;
import org.apache.tinkerpop.machine.functions.NestedFunction;
import org.apache.tinkerpop.machine.traversers.Traverser;

import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapMap<C, S, E> extends AbstractFunction<C> implements MapFunction<C, S, E>, NestedFunction<C, S, E> {

    private final List<CFunction<C>> mapFunctions;
    private Processor<C, S, E> processor;

    public MapMap(final C coefficient, final Set<String> labels, final List<CFunction<C>> mapFunctions) {
        super(coefficient, labels);
        this.mapFunctions = mapFunctions;
    }

    @Override
    public Traverser<C, E> apply(final Traverser<C, S> traverser) {
        this.processor.reset();
        this.processor.addStart(traverser);
        return traverser.split(this.coefficient, this.processor.nextTraverser().object());
    }

    public void setProcessor(final Processor<C, S, E> processor) {
        this.processor = processor;
    }

    public List<CFunction<C>> getFunctions() {
        return this.mapFunctions;
    }

    @Override
    public String toString() {
        return super.toString() + this.processor;
    }
}
