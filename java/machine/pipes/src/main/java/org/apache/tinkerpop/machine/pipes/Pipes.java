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

import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.function.BarrierFunction;
import org.apache.tinkerpop.machine.function.BranchFunction;
import org.apache.tinkerpop.machine.function.CFunction;
import org.apache.tinkerpop.machine.function.FilterFunction;
import org.apache.tinkerpop.machine.function.FlatMapFunction;
import org.apache.tinkerpop.machine.function.InitialFunction;
import org.apache.tinkerpop.machine.function.MapFunction;
import org.apache.tinkerpop.machine.function.ReduceFunction;
import org.apache.tinkerpop.machine.function.branch.RepeatBranch;
import org.apache.tinkerpop.machine.pipes.util.InMemoryReducer;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.traverser.Traverser;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Pipes<C, S, E> implements Processor<C, S, E> {

    private final List<Step<?, ?, ?>> steps = new ArrayList<>();
    private Step<C, ?, E> endStep;
    private SourceStep<C, S> startStep;

    public Pipes(final Compilation<C, S, E> compilation) {
        Step<C, ?, ?> previousStep = EmptyStep.instance();
        for (final CFunction<?> function : compilation.getFunctions()) {
            final Step nextStep;
            if (this.steps.isEmpty() && !(function instanceof InitialFunction)) {
                this.startStep = new SourceStep<>();
                this.steps.add(this.startStep);
                previousStep = this.startStep;
            }

            if (function instanceof RepeatBranch)
                nextStep = new RepeatStep(previousStep, (RepeatBranch<C, ?>) function);
            else if (function instanceof BranchFunction)
                nextStep = new BranchStep(previousStep, (BranchFunction<C, ?, ?, ?>) function);
            else if (function instanceof FilterFunction)
                nextStep = new FilterStep(previousStep, (FilterFunction<C, ?>) function);
            else if (function instanceof FlatMapFunction)
                nextStep = new FlatMapStep(previousStep, (FlatMapFunction<C, ?, ?>) function);
            else if (function instanceof MapFunction)
                nextStep = new MapStep(previousStep, (MapFunction<C, ?, ?>) function);
            else if (function instanceof InitialFunction)
                nextStep = new InitialStep((InitialFunction<C, S>) function, compilation.getTraverserFactory());
            else if (function instanceof BarrierFunction)
                nextStep = new BarrierStep(previousStep, (BarrierFunction) function);
            else if (function instanceof ReduceFunction)
                nextStep = new ReduceStep(previousStep, (ReduceFunction<C, ?, ?>) function,
                        new InMemoryReducer((ReduceFunction<C, ?, ?>) function), compilation.getTraverserFactory());
            else
                throw new RuntimeException("You need a new step type:" + function);

            this.steps.add(nextStep);
            previousStep = nextStep;
        }
        this.endStep = (Step<C, ?, E>) previousStep;
    }

    @Override
    public void addStart(final Traverser<C, S> traverser) {
        this.startStep.addStart(traverser);
    }

    @Override
    public Traverser<C, E> next() {
        return this.endStep.next();
    }

    @Override
    public boolean hasNext() {
        return this.endStep.hasNext();
    }

    @Override
    public void reset() {
        for (final Step<?, ?, ?> step : this.steps) {
            step.reset();
        }
    }

    @Override
    public String toString() {
        return this.steps.toString();
    }
}
