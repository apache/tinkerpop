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

import org.apache.tinkerpop.machine.coefficients.Coefficient;
import org.apache.tinkerpop.machine.functions.AbstractFunction;
import org.apache.tinkerpop.machine.functions.CFunction;
import org.apache.tinkerpop.machine.functions.MapFunction;
import org.apache.tinkerpop.machine.functions.NestedFunction;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.machine.traversers.TraverserFactory;
import org.apache.tinkerpop.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapMap<C, S, E> extends AbstractFunction<C, S, E> implements MapFunction<C, S, E>, NestedFunction.Internal<C, S, E> {

    private final List<CFunction<C>> mapFunctions;
    private TraverserFactory<C> traverserFactory;
    private ProcessorFactory processorFactory;

    private transient Processor<C, S, E> processor;

    public MapMap(final Coefficient<C> coefficient, final Set<String> labels, final List<CFunction<C>> mapFunctions) {
        super(coefficient, labels);
        this.mapFunctions = mapFunctions;
    }

    @Override
    public E apply(final Traverser<C, S> traverser) {
        if (null == this.processor)
            this.processor = processorFactory.mint(traverserFactory, this.mapFunctions);
        else
            this.processor.reset();
        this.processor.addStart(traverser);
        return this.processor.next().object();
    }

    @Override
    public void setProcessor(final TraverserFactory<C> traverserFactory, final ProcessorFactory processorFactory) {
        this.traverserFactory = traverserFactory;
        this.processorFactory = processorFactory;
    }

    @Override
    public List<List<CFunction<C>>> getFunctions() {
        return Collections.singletonList(this.mapFunctions);
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.mapFunctions.toArray());
    }
}
