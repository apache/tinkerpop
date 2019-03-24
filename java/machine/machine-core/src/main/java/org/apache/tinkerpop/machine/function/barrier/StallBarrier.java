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
package org.apache.tinkerpop.machine.function.barrier;

import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.BarrierFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserSet;

import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StallBarrier<C, S> extends AbstractFunction<C> implements BarrierFunction<C, S, Traverser<C, S>, TraverserSet<C, S>> {

    private final int drainThreshold;

    private StallBarrier(final Coefficient<C> coefficient, final String label, final int drainThreshold) {
        super(coefficient, label);
        this.drainThreshold = drainThreshold;
    }


    @Override
    public TraverserSet<C, S> apply(final Traverser<C, S> traverser, final TraverserSet<C, S> traverserSet) {
        traverserSet.add(traverser);
        return traverserSet;
    }

    @Override
    public TraverserSet<C, S> getInitialValue() {
        return new TraverserSet<>();
    }

    @Override
    public Iterator<Traverser<C, S>> createIterator(final TraverserSet<C, S> barrier) {
        return barrier.iterator();
    }

    @Override
    public boolean returnsTraversers() {
        return true;
    }

    public static <C, S> StallBarrier<C, S> compile(final Instruction<C> instruction) {
        return new StallBarrier<>(instruction.coefficient(), instruction.label(), 1000); // TODO
    }
}
