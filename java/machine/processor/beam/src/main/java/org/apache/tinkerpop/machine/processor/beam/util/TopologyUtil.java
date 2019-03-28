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
package org.apache.tinkerpop.machine.processor.beam.util;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
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
import org.apache.tinkerpop.machine.processor.beam.BarrierFn;
import org.apache.tinkerpop.machine.processor.beam.Beam;
import org.apache.tinkerpop.machine.processor.beam.BranchFn;
import org.apache.tinkerpop.machine.processor.beam.FilterFn;
import org.apache.tinkerpop.machine.processor.beam.FlatMapFn;
import org.apache.tinkerpop.machine.processor.beam.InitialFn;
import org.apache.tinkerpop.machine.processor.beam.MapFn;
import org.apache.tinkerpop.machine.processor.beam.ReduceFn;
import org.apache.tinkerpop.machine.processor.beam.RepeatDeadEndFn;
import org.apache.tinkerpop.machine.processor.beam.RepeatEndFn;
import org.apache.tinkerpop.machine.processor.beam.RepeatStartFn;
import org.apache.tinkerpop.machine.processor.beam.io.TraverserCoder;
import org.apache.tinkerpop.machine.processor.beam.io.TraverserSetCoder;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TopologyUtil {

    public static <C, S, E> PCollection<Traverser<C, E>> compile(final PCollection<Traverser<C, S>> source, final Compilation<C, S, E> compilation) {
        final TraverserFactory<C> traverserFactory = compilation.getTraverserFactory();
        PCollection<Traverser<C, E>> sink = (PCollection) source;
        for (final CFunction<C> function : compilation.getFunctions()) {
            sink = TopologyUtil.extend(sink, function, traverserFactory);
        }
        return sink;
    }

    private static <C, S, E, M> PCollection<Traverser<C, E>> extend(final PCollection<Traverser<C, S>> source, final CFunction<C> function, final TraverserFactory<C> traverserFactory) {
        PCollection sink;
        if (function instanceof MapFunction) {
            sink = source.apply(ParDo.of(new MapFn<>((MapFunction<C, S, E>) function)));
        } else if (function instanceof FilterFunction) {
            sink = source.apply(ParDo.of(new FilterFn<>((FilterFunction<C, S>) function)));
        } else if (function instanceof FlatMapFunction) {
            sink = source.apply(ParDo.of(new FlatMapFn<>((FlatMapFunction<C, S, E>) function)));
        } else if (function instanceof InitialFunction) {
            sink = source.apply(ParDo.of(new InitialFn<>((InitialFunction<C, S>) function, traverserFactory)));
        } else if (function instanceof ReduceFunction) {
            sink = source.apply(Combine.globally(new ReduceFn<>((ReduceFunction<C, S, E>) function, traverserFactory)));
        } else if (function instanceof BarrierFunction) {
            sink = source.apply(Combine.globally(new BarrierFn<>((BarrierFunction<C, S, E, M>) function)));
            sink.setCoder(new TraverserSetCoder<>()); // TODO: generalize to any Barrier (this will be hard)
            sink = (PCollection) sink.apply(ParDo.of(new BarrierFn.BarrierIterateFn<>((BarrierFunction<C, S, E, M>) function, traverserFactory)));
        } else if (function instanceof RepeatBranch) {
            final RepeatBranch<C, S> repeatFunction = (RepeatBranch<C, S>) function;
            final List<PCollection<Traverser<C, S>>> repeatOutputs = new ArrayList<>();
            final TupleTag<Traverser<C, S>> repeatDone = new TupleTag<>();
            final TupleTag<Traverser<C, S>> repeatLoop = new TupleTag<>();
            sink = source;
            for (int i = 0; i < Beam.MAX_REPETIONS; i++) {
                if (repeatFunction.hasStartPredicates()) {
                    final RepeatStartFn<C, S> startFn = new RepeatStartFn<>(repeatFunction, repeatDone, repeatLoop);
                    final PCollectionTuple outputs = (PCollectionTuple) sink.apply(ParDo.of(startFn).withOutputTags(repeatLoop, TupleTagList.of(repeatDone)));
                    outputs.getAll().values().forEach(c -> c.setCoder(new TraverserCoder()));
                    repeatOutputs.add(outputs.get(repeatDone));
                    sink = outputs.get(repeatLoop);
                }
                sink = TopologyUtil.compile(sink, repeatFunction.getRepeat());
                if (repeatFunction.hasEndPredicates()) {
                    final RepeatEndFn<C, S> endFn = new RepeatEndFn<>(repeatFunction, repeatDone, repeatLoop);
                    final PCollectionTuple outputs = (PCollectionTuple) sink.apply(ParDo.of(endFn).withOutputTags(repeatLoop, TupleTagList.of(repeatDone)));
                    outputs.getAll().values().forEach(c -> c.setCoder(new TraverserCoder()));
                    repeatOutputs.add(outputs.get(repeatDone));
                    sink = outputs.get(repeatLoop);
                }
            }
            sink = (PCollection<Traverser<C, S>>) sink.apply(ParDo.of(new RepeatDeadEndFn<>()));
            sink.setCoder(new TraverserCoder<>());
            sink = PCollectionList.of(repeatOutputs).apply(Flatten.pCollections());
        } else if (function instanceof BranchFunction) {
            final BranchFunction<C, S, E> branchFunction = (BranchFunction<C, S, E>) function;
            final Map<Compilation<C, S, ?>, List<TupleTag<Traverser<C, S>>>> selectors = new LinkedHashMap<>();
            for (final Map.Entry<Compilation<C, S, ?>, List<Compilation<C, S, E>>> branch : branchFunction.getBranches().entrySet()) {
                final List<TupleTag<Traverser<C, S>>> tags = new ArrayList<>();
                for (final Compilation<C, S, E> temp : branch.getValue()) {
                    tags.add(new TupleTag<>());
                }
                selectors.put(branch.getKey(), tags);
            }
            final BranchFn<C, S, E> fn = new BranchFn<>(branchFunction, selectors);
            final List<TupleTag<Traverser<C, S>>> tags = selectors.values().stream().flatMap(List::stream).collect(Collectors.toList());
            final PCollectionTuple outputs = source.apply(ParDo.of(fn).withOutputTags(tags.get(0), TupleTagList.of((List) tags.subList(1, tags.size()))));
            outputs.getAll().values().forEach(c -> c.setCoder(new TraverserCoder()));
            final List<PCollection<Traverser<C, E>>> branchSinks = new ArrayList<>();
            for (final Map.Entry<Compilation<C, S, ?>, List<Compilation<C, S, E>>> branches : branchFunction.getBranches().entrySet()) {
                for (final TupleTag<Traverser<C, S>> tag : selectors.get(branches.getKey())) {
                    final PCollection<Traverser<C, S>> output = outputs.get(tag);
                    for (final Compilation<C, S, E> branch : branches.getValue()) {
                        branchSinks.add(TopologyUtil.compile(output, branch));
                    }
                }
            }
            sink = PCollectionList.of(branchSinks).apply(Flatten.pCollections());
        } else
            throw new RuntimeException("You need a new step type:" + function);
        sink.setCoder(new TraverserCoder<>());
        return sink;
    }
}
