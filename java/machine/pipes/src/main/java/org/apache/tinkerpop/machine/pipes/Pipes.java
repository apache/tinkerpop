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

import org.apache.tinkerpop.machine.Processor;
import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.apache.tinkerpop.machine.functions.CFunction;
import org.apache.tinkerpop.machine.functions.FilterFunction;
import org.apache.tinkerpop.machine.functions.InitialFunction;
import org.apache.tinkerpop.machine.functions.MapFunction;
import org.apache.tinkerpop.machine.functions.NestedFunction;
import org.apache.tinkerpop.machine.traversers.Traverser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Pipes<C, S, E> implements Iterator<E>, Processor<C, S, E> {

    private final List<AbstractStep<?, ?, ?>> steps = new ArrayList<>();
    private AbstractStep<C, ?, E> endStep;
    private AbstractStep<C, S, ?> startStep = EmptyStep.instance();

    private Pipes(final List<CFunction<C>> functions) {
        AbstractStep previousStep = EmptyStep.instance();
        for (final CFunction<?> function : functions) {
            if (function instanceof NestedFunction)
                ((NestedFunction<C, S, E>) function).setProcessor(new Pipes<>(((NestedFunction<C, S, E>) function).getFunctions()));
            final AbstractStep nextStep;
            if (function instanceof FilterFunction)
                nextStep = new FilterStep<>(previousStep, (FilterFunction<C, ?>) function);
            else if (function instanceof MapFunction)
                nextStep = new MapStep<>(previousStep, (MapFunction<C, ?, ?>) function);
            else if (function instanceof InitialFunction)
                nextStep = new InitialStep<>((InitialFunction<C, S>) function);
            else
                throw new RuntimeException("You need a new step type:" + function);

            if (EmptyStep.instance() == this.startStep)
                this.startStep = nextStep;

            this.steps.add(nextStep);
            previousStep = nextStep;
        }
        this.endStep = previousStep;
    }

    public Pipes(final Bytecode<C> bytecode) throws Exception {
        this(BytecodeUtil.compile(BytecodeUtil.optimize(bytecode)));
    }

    @Override
    public boolean hasNext() {
        return this.endStep.hasNext();
    }

    @Override
    public E next() {
        return this.endStep.next().object();
    }

    public List<E> toList() {
        final List<E> list = new ArrayList<>();
        while (this.hasNext()) {
            list.add(this.next());
        }
        return list;
    }

    @Override
    public void addStart(final Traverser<C, S> traverser) {
        this.startStep.addTraverser(traverser);
    }

    @Override
    public Traverser<C, E> nextTraverser() {
        return this.endStep.next();
    }

    @Override
    public boolean hasNextTraverser() {
        return this.endStep.hasNext();
    }

    @Override
    public void reset() {
        while (hasNext())
            next();
    }

    @Override
    public String toString() {
        return this.steps.toString();
    }
}
