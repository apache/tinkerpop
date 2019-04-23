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
import io.reactivex.functions.Consumer;
import io.reactivex.parallel.ParallelFlowable;
import io.reactivex.processors.PublishProcessor;
import io.reactivex.schedulers.Schedulers;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ParallelRxJava<C, S, E> extends AbstractRxJava<C, S, E> {

    private final ExecutorService threadPool;
    private final String bytecodeId;
    private final ParallelFlowable<Traverser<C, E>> flowable;

    ParallelRxJava(final Compilation<C, S, E> compilation, final ExecutorService threadPool) {
        super(compilation);
        this.threadPool = threadPool;
        this.bytecodeId = compilation.getBytecode().getParent().isEmpty() ?
                (String) BytecodeUtil.getSourceInstructions(compilation.getBytecode(), RxJavaProcessor.RX_ROOT_BYTECODE_ID).get(0).args()[0] :
                null;
        // compile once and use many times
        this.flowable = this.compile(ParallelFlowable.from(Flowable.fromIterable(this.starts)).runOn(Schedulers.from(this.threadPool)), this.compilation);
    }

    @Override
    protected void prepareFlow(final Iterator<Traverser<C, S>> starts, final Consumer<? super Traverser<C, E>> consumer) {
        super.prepareFlow(starts, consumer);
        this.disposable = this.flowable
                .doOnNext(consumer)
                .sequential()
                .subscribeOn(Schedulers.newThread()) // don't block the execution so results can be streamed back in real-time
                .doFinally(() -> {
                    if (null != this.bytecodeId) { // only the parent compilation should close the thread pool
                        RxJavaProcessor.THREAD_POOLS.remove(this.bytecodeId);
                        this.threadPool.shutdown();
                    }
                    this.running.set(Boolean.FALSE);
                })
                .subscribe();
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
            final FlatMapFlow<C, S, E> flatMapFlow = new FlatMapFlow<>((FlatMapFunction<C, S, E>) function);
            return flow.flatMap(t -> Flowable.fromIterable(flatMapFlow.apply(t)));
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
                    final RepeatStart<C, S> repeatStart = new RepeatStart<>(repeatBranch);
                    selectorFlow = flow.flatMap(t -> Flowable.fromIterable(repeatStart.apply(t)));
                    outputs.add(selectorFlow.filter(list -> list.get(0).equals(0)).map(list -> (Traverser<C, S>) list.get(1)).sequential());
                    flow = this.compile(selectorFlow.filter(list -> list.get(0).equals(1)).map(list -> (Traverser<C, S>) list.get(1)), (Compilation) repeatBranch.getRepeat());
                } else
                    flow = this.compile(flow, (Compilation) repeatBranch.getRepeat());
                ///
                if (repeatBranch.hasEndPredicates()) {
                    final RepeatEnd<C, S> repeatEnd = new RepeatEnd<>(repeatBranch);
                    selectorFlow = flow.flatMap(t -> Flowable.fromIterable(repeatEnd.apply(t)));
                    outputs.add(selectorFlow.filter(list -> list.get(0).equals(0)).map(list -> (Traverser<C, S>) list.get(1)).sequential());
                    flow = selectorFlow.filter(list -> list.get(0).equals(1)).map(list -> (Traverser<C, S>) list.get(1));
                } else
                    flow = flow.map(t -> t.repeatLoop(repeatBranch));
            }
            return (ParallelFlowable) PublishProcessor.merge(outputs).parallel().runOn(Schedulers.from(this.threadPool));
        }
        throw new RuntimeException("Need a new execution plan step: " + function);
    }
}
