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
package org.apache.tinkerpop.machine.beam;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.tinkerpop.machine.functions.ReduceFunction;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.machine.traversers.TraverserFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReduceFn<C, S, E> extends Combine.CombineFn<Traverser<C, S>, BasicAccumulator<C, S, E>, Traverser<C, E>> implements Fn<C, S, E> {

    private final ReduceFunction<C, S, E> reduceFunction;
    private final TraverserFactory<C> traverserFactory;


    public ReduceFn(final ReduceFunction<C, S, E> reduceFunction,
                    final TraverserFactory<C> traverserFactory) {
        this.reduceFunction = reduceFunction;
        this.traverserFactory = traverserFactory;
    }

    @Override
    public void addStart(Traverser<C, S> traverser) {

    }


    @Override
    public BasicAccumulator<C, S, E> createAccumulator() {
        return new BasicAccumulator<>(this.reduceFunction, this.traverserFactory);
    }

    @Override
    public BasicAccumulator<C, S, E> addInput(BasicAccumulator<C, S, E> accumulator, Traverser<C, S> input) {
        accumulator.addInput(input);
        return accumulator;
    }

    @Override
    public BasicAccumulator<C, S, E> mergeAccumulators(Iterable<BasicAccumulator<C, S, E>> accumulators) {
        E value = this.reduceFunction.getInitialValue();
        for (final BasicAccumulator accumulator : accumulators) {
            value = this.reduceFunction.merge(value, (E) accumulator.extractOutput().object());
        }

        final BasicAccumulator<C, S, E> accumulator = new BasicAccumulator<>(this.reduceFunction, this.traverserFactory);
        accumulator.setValue(value);
        return accumulator;
    }

    @Override
    public Traverser<C, E> extractOutput(BasicAccumulator<C, S, E> accumulator) {
        return accumulator.extractOutput();
    }

    @Override
    public Coder<BasicAccumulator<C, S, E>> getAccumulatorCoder(CoderRegistry registry, Coder<Traverser<C, S>> inputCoder) throws CannotProvideCoderException {
        return new ReducerCoder<>();
    }

    @Override
    public Coder<Traverser<C, E>> getDefaultOutputCoder(CoderRegistry registry, Coder<Traverser<C, S>> inputCoder) throws CannotProvideCoderException {
        return new TraverserCoder<>();
    }

    @Override
    public String toString() {
        return this.reduceFunction.toString();
    }
}
