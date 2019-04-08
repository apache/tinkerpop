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
import io.reactivex.parallel.ParallelFlowable;
import io.reactivex.processors.PublishProcessor;
import io.reactivex.schedulers.Schedulers;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ParallelRxJava<C, S, E> extends AbstractRxJava<C, S, E> {

    private final int threads;
    private ExecutorService threadPool;

    ParallelRxJava(final Compilation<C, S, E> compilation, final int threads) {
        super(compilation);
        this.threads = threads;
    }

    @Override
    protected void prepareFlow() {
        if (!this.executed) {
            this.executed = true;
            this.alive.set(Boolean.TRUE);
            this.threadPool = Executors.newFixedThreadPool(this.threads);
            this.compile(
                    ParallelFlowable.from(Flowable.fromIterable(this.starts)).
                            runOn(Schedulers.from(this.threadPool)), this.compilation).
                    doOnNext(this.ends::add).
                    sequential().
                    doOnComplete(() -> this.alive.set(Boolean.FALSE)).
                    doFinally(this.threadPool::shutdown).
                    blockingSubscribe(); // thread this so results can be received before computation completes
        }
    }

    // EXECUTION PLAN COMPILER

    private ParallelFlowable<Traverser<C, E>> compile(final ParallelFlowable<Traverser<C, S>> source, final Compilation<C, S, E> compilation) {
        final TraverserFactory<C> traverserFactory = compilation.getTraverserFactory();
        ParallelFlowable<Traverser<C, E>> sink = (ParallelFlowable) source;
        for (final CFunction<C> function : compilation.getFunctions()) {
            sink = this.extend((ParallelFlowable) sink, function, traverserFactory);
        }
        return sink;
    }

    private <B> ParallelFlowable<Traverser<C, E>> extend(ParallelFlowable<Traverser<C, S>> flow, final CFunction<C> function, final TraverserFactory<C> traverserFactory) {
        if (function instanceof MapFunction)
            return flow.map(new MapFlow<>((MapFunction<C, S, E>) function));
        else if (function instanceof FilterFunction) {
            return (ParallelFlowable) flow.filter(new FilterFlow<>((FilterFunction<C, S>) function));
        } else if (function instanceof FlatMapFunction) {
            return flow.sequential().flatMapIterable(new FlatMapFlow<>((FlatMapFunction<C, S, E>) function)).parallel().runOn(Schedulers.from(this.threadPool));
        } else if (function instanceof InitialFunction) {
            return Flowable.fromIterable(() -> IteratorUtils.map(((InitialFunction<C, E>) function).get(), s -> traverserFactory.create(function, s))).parallel().runOn(Schedulers.from(this.threadPool));
        } else if (function instanceof ReduceFunction) {
            final ReduceFunction<C, S, E> reduceFunction = (ReduceFunction<C, S, E>) function;
            return flow.sequential().reduce(traverserFactory.create(reduceFunction, reduceFunction.getInitialValue()), new Reducer<>(reduceFunction)).toFlowable().parallel().runOn(Schedulers.from(this.threadPool));
        } else if (function instanceof BarrierFunction) {
            final BarrierFunction<C, S, E, B> barrierFunction = (BarrierFunction<C, S, E, B>) function;
            return flow.sequential().reduce(barrierFunction.getInitialValue(), new Barrier<>(barrierFunction)).toFlowable().flatMapIterable(new BarrierFlow<>(barrierFunction, traverserFactory)).parallel(1); // order requires serial
        } else if (function instanceof BranchFunction) {
            final ParallelFlowable<List> selectorFlow = flow.map(new BranchFlow<>((BranchFunction<C, S, B>) function));
            final List<Publisher<Traverser<C, E>>> branchFlows = new ArrayList<>();
            int branchCounter = 0;
            for (final Map.Entry<Compilation<C, S, ?>, List<Compilation<C, S, E>>> branches : ((BranchFunction<C, S, E>) function).getBranches().entrySet()) {
                final int branchId = null == branches.getKey() ? -1 : branchCounter;
                branchCounter++;
                for (final Compilation<C, S, E> branch : branches.getValue()) {
                    branchFlows.add(this.compile(selectorFlow.
                                    filter(list -> list.get(0).equals(branchId)).
                                    map(list -> (Traverser<C, S>) list.get(1)),
                            branch).sequential());
                }
            }
            return PublishProcessor.merge(branchFlows).parallel().runOn(Schedulers.from(this.threadPool));
        } else if (function instanceof RepeatBranch) {
            final RepeatBranch<C, S> repeatBranch = (RepeatBranch<C, S>) function;
            final List<Publisher<Traverser<C, S>>> outputs = new ArrayList<>();
            ParallelFlowable<List> selectorFlow;
            for (int i = 0; i < MAX_REPETITIONS; i++) {
                if (repeatBranch.hasStartPredicates()) {
                    selectorFlow = flow.sequential().flatMapIterable(new RepeatStart<>(repeatBranch)).parallel().runOn(Schedulers.from(this.threadPool));
                    outputs.add(selectorFlow.filter(list -> list.get(0).equals(0)).map(list -> (Traverser<C, S>) list.get(1)).sequential());
                    flow = this.compile(selectorFlow.filter(list -> list.get(0).equals(1)).map(list -> (Traverser<C, S>) list.get(1)), (Compilation) repeatBranch.getRepeat());
                } else
                    flow = this.compile(flow, (Compilation) repeatBranch.getRepeat());
                selectorFlow = flow.sequential().flatMapIterable(new RepeatEnd<>(repeatBranch)).parallel().runOn(Schedulers.from(this.threadPool));
                outputs.add(selectorFlow.sequential().filter(list -> list.get(0).equals(0)).map(list -> (Traverser<C, S>) list.get(1)));
                flow = selectorFlow.filter(list -> list.get(0).equals(1)).map(list -> (Traverser<C, S>) list.get(1));
            }
            return (ParallelFlowable) PublishProcessor.merge(outputs).parallel().runOn(Schedulers.from(this.threadPool));
        }
        throw new RuntimeException("Need a new execution plan step: " + function);
    }
}
