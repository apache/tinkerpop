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
package org.apache.tinkerpop.machine.pipes;

import org.apache.tinkerpop.machine.functions.ReduceFunction;
import org.apache.tinkerpop.machine.functions.reduce.Reducer;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.machine.traversers.TraverserFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReduceStep<C, S, E> extends AbstractStep<C, S, E> {

    private final ReduceFunction<C, S, E> reduceFunction;
    private final Reducer<E> reducer;
    private final TraverserFactory<C, E> traverserFactory;
    private boolean done = false;

    public ReduceStep(final AbstractStep<C, ?, S> previousStep,
                      final ReduceFunction<C, S, E> reduceFunction,
                      final Reducer<E> reducer,
                      final TraverserFactory<C, E> traverserFactory) {
        super(previousStep, reduceFunction);
        this.reduceFunction = reduceFunction;
        this.reducer = reducer;
        this.traverserFactory = traverserFactory;
    }

    @Override
    public Traverser<C, E> next() {
        this.done = true;
        Traverser<C, S> traverser = null;
        while (this.hasNext()) {
            traverser = getPreviousTraverser();
            this.reducer.update(this.reduceFunction.apply(traverser, this.reducer.get()));
        }
        return null == traverser ?
                this.traverserFactory.create(this.function.coefficient(), this.reduceFunction.getInitialValue()) :
                traverser.reduce(this.reducer);
    }

    @Override
    public boolean hasNext() {
        return !this.done;
    }
}
