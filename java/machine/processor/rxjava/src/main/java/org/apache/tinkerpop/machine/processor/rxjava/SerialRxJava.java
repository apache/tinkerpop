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
package org.apache.tinkerpop.machine.processor.rxjava;

import io.reactivex.Flowable;
import io.reactivex.processors.PublishProcessor;
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.function.BarrierFunction;
import org.apache.tinkerpop.machine.function.BranchFunction;
import org.apache.tinkerpop.machine.function.CFunction;
import org.apache.tinkerpop.machine.function.FilterFunction;
import org.apache.tinkerpop.machine.function.FlatMapFunction;
import org.apache.tinkerpop.machine.function.InitialFunction;
import org.apache.tinkerpop.machine.function.MapFunction;
import org.apache.tinkerpop.machine.function.ReduceFunction;
import org.apache.tinkerpop.machine.function.branch.RepeatBranch;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserFactory;
import org.apache.tinkerpop.machine.util.IteratorUtils;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SerialRxJava<C, S, E> extends AbstractRxJava<C, S, E> {

    SerialRxJava(final Compilation<C, S, E> compilation) {
        super(compilation);
    }

    @Override
    protected void prepareFlow() {
        if (!this.executed) {
            this.executed = true;
            this.alive.set(Boolean.TRUE);
            SerialRxJava.compile(Flowable.fromIterable(this.starts), this.compilation).
                    doOnNext(this.ends::add).
                    doOnComplete(() -> this.alive.set(Boolean.FALSE)).
                    subscribe();
        }
        while (this.alive.get() && this.ends.isEmpty()) {
            // only return if there is a result ready from the flow (or the flow is dead)
        }
    }

    // EXECUTION PLAN COMPILER

    private static <C, S, E> Flowable<Traverser<C, E>> compile(final Flowable<Traverser<C, S>> source, final Compilation<C, S, E> compilation) {
        final TraverserFactory<C> traverserFactory = compilation.getTraverserFactory();
        Flowable<Traverser<C, E>> sink = (Flowable) source;
        for (final CFunction<C> function : compilation.getFunctions()) {
            sink = SerialRxJava.extend(sink, function, traverserFactory);
        }
        return sink;
    }

    private static <C, S, E, B> Flowable<Traverser<C, E>> extend(Flowable<Traverser<C, S>> flow, final CFunction<C> function, final TraverserFactory<C> traverserFactory) {
        if (function instanceof MapFunction)
            return flow.map(new MapFlow<>((MapFunction<C, S, E>) function));
        else if (function instanceof FilterFunction) {
            return (Flowable) flow.filter(new FilterFlow<>((FilterFunction<C, S>) function));
        } else if (function instanceof FlatMapFunction) {
            return flow.flatMapIterable(new FlatMapFlow<>((FlatMapFunction<C, S, E>) function));
        } else if (function instanceof InitialFunction) {
            return Flowable.fromIterable(() -> IteratorUtils.map(((InitialFunction<C, E>) function).get(), s -> traverserFactory.create(function, s)));
        } else if (function instanceof ReduceFunction) {
            final ReduceFunction<C, S, E> reduceFunction = (ReduceFunction<C, S, E>) function;
            return flow.reduce(traverserFactory.create(reduceFunction, reduceFunction.getInitialValue()), new Reducer<>(reduceFunction)).toFlowable();
        } else if (function instanceof BarrierFunction) {
            final BarrierFunction<C, S, E, B> barrierFunction = (BarrierFunction<C, S, E, B>) function;
            return flow.reduce(barrierFunction.getInitialValue(), new Barrier<>(barrierFunction)).toFlowable().flatMapIterable(new BarrierFlow<>(barrierFunction, traverserFactory));
        } else if (function instanceof BranchFunction) {
            final Flowable<List> selectorFlow = flow.map(new BranchFlow<>((BranchFunction<C, S, B>) function));
            final List<Publisher<Traverser<C, E>>> branchFlows = new ArrayList<>();
            int branchCounter = 0;
            for (final Map.Entry<Compilation<C, S, ?>, List<Compilation<C, S, E>>> branches : ((BranchFunction<C, S, E>) function).getBranches().entrySet()) {
                final int branchId = null == branches.getKey() ? -1 : branchCounter;
                branchCounter++;
                for (final Compilation<C, S, E> branch : branches.getValue()) {
                    branchFlows.add(compile(selectorFlow.
                                    filter(list -> list.get(0).equals(branchId)).
                                    map(list -> (Traverser<C, S>) list.get(1)),
                            branch));
                }
            }
            return PublishProcessor.merge(branchFlows);
        } else if (function instanceof RepeatBranch) {
            final RepeatBranch<C, S> repeatBranch = (RepeatBranch<C, S>) function;
            final List<Publisher<Traverser<C, S>>> outputs = new ArrayList<>();
            Flowable<List> selectorFlow;
            for (int i = 0; i < MAX_REPETITIONS; i++) {
                if (repeatBranch.hasStartPredicates()) {
                    selectorFlow = flow.flatMapIterable(new RepeatStart<>(repeatBranch));
                    outputs.add(selectorFlow.filter(list -> list.get(0).equals(0)).map(list -> (Traverser<C, S>) list.get(1)));
                    flow = compile(selectorFlow.filter(list -> list.get(0).equals(1)).map(list -> (Traverser<C, S>) list.get(1)), repeatBranch.getRepeat());
                } else
                    flow = compile(flow, repeatBranch.getRepeat());
                selectorFlow = flow.flatMapIterable(new RepeatEnd<>(repeatBranch));
                outputs.add(selectorFlow.filter(list -> list.get(0).equals(0)).map(list -> (Traverser<C, S>) list.get(1)));
                flow = selectorFlow.filter(list -> list.get(0).equals(1)).map(list -> (Traverser<C, S>) list.get(1));
            }
            return (Flowable) PublishProcessor.merge(outputs);
        }
        throw new RuntimeException("Need a new execution plan step: " + function);
    }
}
