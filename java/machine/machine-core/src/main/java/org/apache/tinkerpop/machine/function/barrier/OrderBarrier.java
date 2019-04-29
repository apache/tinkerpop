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
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.bytecode.compiler.CompilationCircle;
import org.apache.tinkerpop.machine.bytecode.compiler.Order;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.BarrierFunction;
import org.apache.tinkerpop.machine.processor.util.FilterProcessor;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserSet;
import org.apache.tinkerpop.machine.traverser.species.ProjectedTraverser;
import org.apache.tinkerpop.machine.util.IteratorUtils;
import org.apache.tinkerpop.machine.util.MultiComparator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderBarrier<C, S, P> extends AbstractFunction<C> implements BarrierFunction<C, S, Traverser<C, S>, TraverserSet<C, S>> {

    private MultiComparator<P> comparator;
    private final CompilationCircle<C, S, P> compilationCircle;

    private OrderBarrier(final Coefficient<C> coefficient, final String label, final MultiComparator<P> comparator, final CompilationCircle<C, S, P> compilationCircle) {
        super(coefficient, label);
        this.comparator = comparator;
        this.compilationCircle = compilationCircle;
    }

    @Override
    public TraverserSet<C, S> getInitialValue() {
        return new TraverserSet<>();
    }

    @Override
    public TraverserSet<C, S> merge(final TraverserSet<C, S> barrierA, final TraverserSet<C, S> barrierB) {
        barrierA.addAll(barrierB);
        return barrierA;
    }

    @Override
    public Iterator<Traverser<C, S>> createIterator(final TraverserSet<C, S> barrier) {
        barrier.sort((Comparator) this.comparator);
        return IteratorUtils.map(barrier.iterator(), t -> ((ProjectedTraverser<C, S, P>) t).getBaseTraverser());
    }

    @Override
    public boolean returnsTraversers() {
        return true;
    }

    @Override
    public TraverserSet<C, S> apply(final Traverser<C, S> traverser, final TraverserSet<C, S> traverserSet) {
        final List<P> projections = new ArrayList<>();
        this.compilationCircle.reset();
        for (int i = 0; i < compilationCircle.size(); i++) {
            projections.add(this.compilationCircle.processTraverser(traverser));
        }
        traverserSet.add(new ProjectedTraverser<>(traverser, projections));
        return traverserSet;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.comparator.hashCode() ^ this.compilationCircle.hashCode();
    }

    public static <C, S, P> OrderBarrier<C, S, P> compile(final Instruction<C> instruction) {
        final List<Compilation<C, S, P>> compilations = new ArrayList<>();
        final List<Comparator<P>> comparators = new ArrayList<>();

        for (int i = 0; i < instruction.args().length; i = i + 2) {
            compilations.add(Compilation.compile(instruction.args()[i]));
            comparators.add((Comparator<P>) Order.valueOf(instruction.args()[i + 1]));
        }

        if (comparators.isEmpty()) {
            compilations.add(new Compilation<>(new FilterProcessor(true)));
            comparators.add((Comparator<P>) Order.asc);
        }

        return new OrderBarrier<>(instruction.coefficient(), instruction.label(), new MultiComparator<>(comparators), new CompilationCircle<>(compilations));
    }

}
