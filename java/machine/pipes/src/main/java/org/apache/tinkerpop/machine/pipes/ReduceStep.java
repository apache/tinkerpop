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

import org.apache.tinkerpop.machine.function.ReduceFunction;
import org.apache.tinkerpop.machine.pipes.util.Reducer;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class ReduceStep<C, S, E> extends AbstractStep<C, S, E> {

    private final ReduceFunction<C, S, E> reduceFunction;
    private final Reducer<C, S, E> reducer;
    private final TraverserFactory<C> traverserFactory;
    private boolean done = false;

    ReduceStep(final Step<C, ?, S> previousStep,
               final ReduceFunction<C, S, E> reduceFunction,
               final Reducer<C, S, E> reducer,
               final TraverserFactory<C> traverserFactory) {
        super(previousStep, reduceFunction);
        this.reduceFunction = reduceFunction;
        this.reducer = reducer;
        this.traverserFactory = traverserFactory;
    }

    @Override
    public Traverser<C, E> next() {
        while (this.previousStep.hasNext()) {
            this.reducer.add(super.previousStep.next());
        }
        this.done = true;
        return this.traverserFactory.create(this.reduceFunction.coefficient(), this.reducer.get());
    }

    @Override
    public boolean hasNext() {
        return !this.done && this.previousStep.hasNext();
    }

    @Override
    public void reset() {
        this.reducer.reset();
        this.done = false;
    }
}
