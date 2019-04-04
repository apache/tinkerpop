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
package org.apache.tinkerpop.machine.processor.beam.sideeffect;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.tinkerpop.machine.function.BarrierFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class InMemoryBarrier<C, S, E, B> implements Combine.AccumulatingCombineFn.Accumulator<Traverser<C, S>, InMemoryBarrier<C, S, E, B>, B>, Serializable {

    private B barrier;
    private BarrierFunction<C, S, E, B> barrierFunction;

    public InMemoryBarrier(final B value, final BarrierFunction<C, S, E, B> barrierFunction) {
        this.barrier = value;
        this.barrierFunction = barrierFunction;
    }

    public InMemoryBarrier(final BarrierFunction<C, S, E, B> barrierFunction) {
        this(barrierFunction.getInitialValue(), barrierFunction);
    }

    @Override
    public void addInput(final Traverser<C, S> input) {
        this.barrierFunction.apply(input.clone(), this.barrier); // clone is necessary given Beam's mutability constraint
    }

    @Override
    public void mergeAccumulator(final InMemoryBarrier<C, S, E, B> other) {
        this.barrierFunction.merge(this.barrier, other.barrier);
    }

    @Override
    public B extractOutput() {
        return this.barrier;
    }

}
