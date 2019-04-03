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
package org.apache.tinkerpop.machine.processor.rxjava.util;

import io.reactivex.Flowable;
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.function.BranchFunction;
import org.apache.tinkerpop.machine.function.CFunction;
import org.apache.tinkerpop.machine.function.FilterFunction;
import org.apache.tinkerpop.machine.function.FlatMapFunction;
import org.apache.tinkerpop.machine.function.InitialFunction;
import org.apache.tinkerpop.machine.function.MapFunction;
import org.apache.tinkerpop.machine.function.ReduceFunction;
import org.apache.tinkerpop.machine.processor.rxjava.BranchFlow;
import org.apache.tinkerpop.machine.processor.rxjava.FilterFlow;
import org.apache.tinkerpop.machine.processor.rxjava.FlatMapFlow;
import org.apache.tinkerpop.machine.processor.rxjava.MapFlow;
import org.apache.tinkerpop.machine.processor.rxjava.ReduceFlow;
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
public final class TopologyUtil {

    public static <C, S, E> Flowable<Traverser<C, E>> compile(final Flowable<Traverser<C, S>> source, final Compilation<C, S, E> compilation) {
        final TraverserFactory<C> traverserFactory = compilation.getTraverserFactory();
        Flowable<Traverser<C, E>> sink = (Flowable) source;
        for (final CFunction<C> function : compilation.getFunctions()) {
            sink = TopologyUtil.extend(sink, function, traverserFactory);
        }
        return sink;
    }

    /*
     private final void stageInput() {
        if (this.hasStartPredicates) {
            final Traverser<C, S> traverser = this.inputTraversers.isEmpty() ? this.previousStep.next() : this.inputTraversers.remove();
            if (1 == this.untilLocation) {
                if (this.untilCompilation.filterTraverser(traverser)) {
                    this.outputTraversers.add(traverser);
                } else if (2 == this.emitLocation && this.emitCompilation.filterTraverser(traverser)) {
                    this.outputTraversers.add(traverser.repeatDone(this.repeatBranch));
                    this.repeat.addTraverser(traverser);
                } else
                    this.repeat.addTraverser(traverser);
            } else if (1 == this.emitLocation) {
                if (this.emitCompilation.filterTraverser(traverser))
                    this.outputTraversers.add(traverser.repeatDone(this.repeatBranch));
                if (2 == this.untilLocation && this.untilCompilation.filterTraverser(traverser))
                    this.outputTraversers.add(traverser.repeatDone(this.repeatBranch));
                else
                    this.repeat.addTraverser(traverser);
            }
        } else {
            this.repeat.addTraverser(this.inputTraversers.isEmpty() ? this.previousStep.next() : this.inputTraversers.remove());
        }
    }


     */

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
            return flow.reduce(traverserFactory.create(reduceFunction, reduceFunction.getInitialValue()), new ReduceFlow<>(reduceFunction)).toFlowable();
        } else if (function instanceof BranchFunction) {
            final Flowable<List> selectorFlow = flow.map(new BranchFlow<>((BranchFunction<C, S, B>) function));
            final List<Publisher<Traverser<C, E>>> branchFlows = new ArrayList<>();
            for (final Map.Entry<Compilation<C, S, ?>, List<Compilation<C, S, E>>> branches : ((BranchFunction<C, S, E>) function).getBranches().entrySet()) {
                for (int i = 0; i < branches.getValue().size(); i++) {
                    final Compilation<C, S, E> branch = branches.getValue().get(i);
                    final int id = i;
                    branchFlows.add(
                            selectorFlow.
                                    filter(list -> list.get(0).equals(null == branches.getKey() ? -1 : id)).
                                    map(list -> (Traverser<C, S>) list.get(1)).
                                    publish(f -> compile(f, branch)));
                }
            }
            Flowable<Traverser<C, E>> sink = (Flowable) flow.filter(t -> false); // branches are the only outputs
            for (final Publisher<Traverser<C, E>> branchFlow : branchFlows) {
                sink = sink.mergeWith(branchFlow);
            }
            return sink;
        }
        throw new RuntimeException("Need a new execution plan step: " + function);
    }
}