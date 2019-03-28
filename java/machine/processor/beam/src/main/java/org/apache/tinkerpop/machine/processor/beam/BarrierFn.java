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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.tinkerpop.machine.function.BarrierFunction;
import org.apache.tinkerpop.machine.processor.beam.io.BarrierCoder;
import org.apache.tinkerpop.machine.processor.beam.io.TraverserSetCoder;
import org.apache.tinkerpop.machine.processor.beam.sideeffect.InMemoryBarrier;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserFactory;
import org.apache.tinkerpop.machine.util.IteratorUtils;

import java.util.Iterator;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BarrierFn<C, S, E, B> extends Combine.CombineFn<Traverser<C, S>, InMemoryBarrier<C, S, E, B>, B> implements Fn {

    private final BarrierFunction<C, S, E, B> barrierFunction;

    public BarrierFn(final BarrierFunction<C, S, E, B> barrierFunction) {
        this.barrierFunction = barrierFunction;
    }

    @Override
    public InMemoryBarrier<C, S, E, B> createAccumulator() {
        return new InMemoryBarrier<>(this.barrierFunction);
    }

    @Override
    public InMemoryBarrier<C, S, E, B> addInput(final InMemoryBarrier<C, S, E, B> accumulator, final Traverser<C, S> input) {
        accumulator.addInput(input);
        return accumulator;
    }

    @Override
    public InMemoryBarrier<C, S, E, B> mergeAccumulators(final Iterable<InMemoryBarrier<C, S, E, B>> accumulators) {
        B barrier = this.barrierFunction.getInitialValue();
        for (final InMemoryBarrier<C, S, E, B> accumulator : accumulators) {
            barrier = this.barrierFunction.merge(barrier, accumulator.extractOutput());
        }
        return new InMemoryBarrier<>(barrier, this.barrierFunction);
    }

    @Override
    public B extractOutput(final InMemoryBarrier<C, S, E, B> accumulator) {
        return accumulator.extractOutput();
    }

    @Override
    public Coder<InMemoryBarrier<C, S, E, B>> getAccumulatorCoder(final CoderRegistry registry, final Coder<Traverser<C, S>> inputCoder) throws CannotProvideCoderException {
        return new BarrierCoder<>();
    }

    @Override
    public Coder<B> getDefaultOutputCoder(final CoderRegistry registry, final Coder<Traverser<C, S>> inputCoder) throws CannotProvideCoderException {
        return (Coder<B>) new TraverserSetCoder<C, S>();
    }

    public static class BarrierIterateFn<C, S, E, B> extends DoFn<B, Traverser<C, S>> {

        private final BarrierFunction<C, S, E, B> barrierFunction;
        private final TraverserFactory traverserFactory;


        public BarrierIterateFn(final BarrierFunction<C, S, E, B> barrierFunction, final TraverserFactory traverserFactory) {
            this.barrierFunction = barrierFunction;
            this.traverserFactory = traverserFactory;
        }

        @ProcessElement
        public void processElement(final @Element B barrier, final OutputReceiver<Traverser<C, S>> output) {
            final B local = this.barrierFunction.merge(this.barrierFunction.getInitialValue(), barrier);
            final Iterator<Traverser<C, S>> iterator = this.barrierFunction.returnsTraversers() ?
                    (Iterator<Traverser<C, S>>) this.barrierFunction.createIterator(local) :
                    IteratorUtils.map(this.barrierFunction.createIterator(local),
                            e -> this.traverserFactory.create(this.barrierFunction, e));
            while (iterator.hasNext())
                output.output(iterator.next());
        }
    }
}
