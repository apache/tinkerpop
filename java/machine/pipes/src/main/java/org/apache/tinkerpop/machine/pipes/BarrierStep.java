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

import org.apache.tinkerpop.machine.functions.BarrierFunction;
import org.apache.tinkerpop.machine.pipes.util.Barrier;
import org.apache.tinkerpop.machine.pipes.util.InMemoryBarrier;
import org.apache.tinkerpop.machine.traversers.Traverser;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BarrierStep<C, S, E, B> extends AbstractStep<C, S, E> {

    private final Barrier<B> barrier;
    private final BarrierFunction<C, S, E, B> barrierFunction;
    private boolean done = false;
    private Iterator<E> output = Collections.emptyIterator();

    BarrierStep(final Step<C, ?, S> previousStep, final BarrierFunction<C, S, E, B> barrierFunction) {
        super(previousStep, barrierFunction);
        this.barrier = new InMemoryBarrier<>(barrierFunction.getInitialValue()); // TODO: move to strategy determination
        this.barrierFunction = barrierFunction;
    }

    @Override
    public Traverser<C, E> next() {
        if (!this.done) {
            while (this.previousStep.hasNext()) {
                this.barrier.update(this.barrierFunction.apply(super.previousStep.next(), this.barrier.get()));
            }
            this.done = true;
            this.output = (Iterator<E>) this.barrierFunction.createIterator(this.barrier.get());
        }
        return (Traverser<C, E>) this.output.next();
    }

    @Override
    public boolean hasNext() {
        return this.output.hasNext() || (!this.done && this.previousStep.hasNext());
    }

    @Override
    public void reset() {
        this.barrier.reset();
        this.done = false;
    }
}
