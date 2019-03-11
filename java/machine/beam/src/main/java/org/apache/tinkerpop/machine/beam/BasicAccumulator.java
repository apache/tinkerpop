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

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.tinkerpop.machine.functions.ReduceFunction;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.machine.traversers.TraverserFactory;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@DefaultCoder(ReducerCoder.class)
public class BasicAccumulator<C, S, E> implements Combine.AccumulatingCombineFn.Accumulator<Traverser<C, S>, BasicAccumulator<C, S, E>, Traverser<C, E>>, Serializable {

    private E value;
    private final ReduceFunction<C, S, E> reduceFunction;
    private final TraverserFactory<C> traverserFactory;

    public BasicAccumulator(final ReduceFunction<C, S, E> reduceFunction, final TraverserFactory<C> traverserFactory) {
        super();
        this.value = reduceFunction.getInitialValue();
        this.reduceFunction = reduceFunction;
        this.traverserFactory = traverserFactory;
    }

    public void setValue(final E value) {
        this.value = value;
    }

    @Override
    public void addInput(final Traverser<C, S> input) {
        this.value = reduceFunction.apply(input, this.value);
    }

    @Override
    public void mergeAccumulator(final BasicAccumulator<C, S, E> other) {
        this.value = this.reduceFunction.apply(this.traverserFactory.create(this.reduceFunction.coefficient(), (S) this.value), other.value);
    }

    @Override
    public Traverser<C, E> extractOutput() {
        return this.traverserFactory.create(this.reduceFunction.coefficient(), this.value);
    }
}
