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
package org.apache.tinkerpop.machine.processor.beam;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.tinkerpop.machine.processor.beam.serialization.ReducerCoder;
import org.apache.tinkerpop.machine.processor.beam.serialization.TraverserCoder;
import org.apache.tinkerpop.machine.processor.beam.sideeffect.BasicReducer;
import org.apache.tinkerpop.machine.function.ReduceFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReduceFn<C, S, E> extends Combine.CombineFn<Traverser<C, S>, BasicReducer<C, S, E>, Traverser<C, E>> implements Fn {

    private final ReduceFunction<C, S, E> reduceFunction;
    private final TraverserFactory<C> traverserFactory;


    public ReduceFn(final ReduceFunction<C, S, E> reduceFunction, final TraverserFactory<C> traverserFactory) {
        this.reduceFunction = reduceFunction;
        this.traverserFactory = traverserFactory;
    }

    @Override
    public BasicReducer<C, S, E> createAccumulator() {
        return new BasicReducer<>(this.reduceFunction, this.traverserFactory);
    }

    @Override
    public BasicReducer<C, S, E> addInput(final BasicReducer<C, S, E> accumulator, final Traverser<C, S> input) {
        accumulator.addInput(input);
        return accumulator;
    }

    @Override
    public BasicReducer<C, S, E> mergeAccumulators(Iterable<BasicReducer<C, S, E>> accumulators) {
        E value = this.reduceFunction.getInitialValue();
        for (final BasicReducer accumulator : accumulators) {
            value = this.reduceFunction.merge(value, (E) accumulator.extractOutput().object());
        }

        final BasicReducer<C, S, E> accumulator = new BasicReducer<>(this.reduceFunction, this.traverserFactory);
        accumulator.setValue(value);
        return accumulator;
    }

    @Override
    public Traverser<C, E> extractOutput(final BasicReducer<C, S, E> accumulator) {
        return accumulator.extractOutput().reduce(this.reduceFunction);
    }

    @Override
    public Coder<BasicReducer<C, S, E>> getAccumulatorCoder(final CoderRegistry registry, final Coder<Traverser<C, S>> inputCoder) throws CannotProvideCoderException {
        return new ReducerCoder<>();
    }

    @Override
    public Coder<Traverser<C, E>> getDefaultOutputCoder(final CoderRegistry registry, final Coder<Traverser<C, S>> inputCoder) throws CannotProvideCoderException {
        return new TraverserCoder<>();
    }

    @Override
    public String toString() {
        return this.reduceFunction.toString();
    }
}
