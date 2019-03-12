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
package org.apache.tinkerpop.machine.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.tinkerpop.machine.beam.serialization.TraverserCoder;
import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.coefficients.Coefficient;
import org.apache.tinkerpop.machine.coefficients.LongCoefficient;
import org.apache.tinkerpop.machine.functions.BranchFunction;
import org.apache.tinkerpop.machine.functions.CFunction;
import org.apache.tinkerpop.machine.functions.FilterFunction;
import org.apache.tinkerpop.machine.functions.FlatMapFunction;
import org.apache.tinkerpop.machine.functions.InitialFunction;
import org.apache.tinkerpop.machine.functions.MapFunction;
import org.apache.tinkerpop.machine.functions.ReduceFunction;
import org.apache.tinkerpop.machine.functions.branch.RepeatBranch;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.machine.traversers.TraverserFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Beam<C, S, E> implements Processor<C, S, E> {

    private final Pipeline pipeline;
    public static List<Traverser> OUTPUT = new ArrayList<>(); // FIX THIS!
    private final List<Fn> functions = new ArrayList<>();
    private Iterator<Traverser<C, E>> iterator = null;

    public Beam(final Compilation<C, S, E> compilation) {

        this.pipeline = Pipeline.create();
        PCollection<Traverser<C, ?>> collection = this.pipeline.apply(Create.of(compilation.getTraverserFactory().create((Coefficient) LongCoefficient.create(), 1L)));
        collection.setCoder(new TraverserCoder());
        for (final CFunction<?> function : compilation.getFunctions()) {
            collection = processFunction(collection, compilation.getTraverserFactory(), function, false);
        }
        collection.apply(ParDo.of(new OutputStep()));
        this.pipeline.getOptions().setRunner(new PipelineOptions.DirectRunner().create(this.pipeline.getOptions()));
    }

    private PCollection<Traverser<C, ?>> processFunction(
            PCollection<Traverser<C, ?>> collection,
            final TraverserFactory<C> traverserFactory,
            final CFunction<?> function,
            final boolean branching) {
        DoFn<Traverser<C, S>, Traverser<C, E>> fn = null;
        if (function instanceof RepeatBranch) {
            final Compilation<C, S, S> repeat = ((RepeatBranch) function).getRepeat();
            final List<PCollection> outputs = new ArrayList<>();
            final TupleTag repeatDone = new TupleTag<>();
            final TupleTag repeatLoop = new TupleTag<>();
            for (int i = 0; i < 10; i++) {
                fn = new RepeatFn((RepeatBranch) function, repeatDone, repeatLoop);
                PCollectionTuple branches = (PCollectionTuple) collection.apply(ParDo.of(fn).withOutputTags(repeatLoop, TupleTagList.of(repeatDone)));
                branches.get(repeatLoop).setCoder(new TraverserCoder());
                branches.get(repeatDone).setCoder(new TraverserCoder());
                outputs.add(branches.get(repeatDone));
                for (final CFunction<C> repeatFunction : repeat.getFunctions()) {
                    collection = this.processFunction(branches.get(repeatLoop), traverserFactory, repeatFunction, true);
                }
            }
            this.functions.add((Fn) fn);
            collection = (PCollection) PCollectionList.of((Iterable) outputs).apply(Flatten.pCollections());
            collection.setCoder(new TraverserCoder());
        } else if (function instanceof BranchFunction) {
            final List<Compilation<C, ?, ?>> branches = ((BranchFunction<C, ?, ?>) function).getInternals();
            final List<PCollection<Traverser<C, ?>>> collections = new ArrayList<>(branches.size());
            for (final Compilation<C, ?, ?> branch : branches) {
                PCollection<Traverser<C, ?>> branchCollection = collection;
                for (final CFunction<C> branchFunction : branch.getFunctions()) {
                    branchCollection = this.processFunction(branchCollection, traverserFactory, branchFunction, true);
                }
                collections.add(branchCollection);
            }
            collection = PCollectionList.of(collections).apply(Flatten.pCollections());
            this.functions.add(new BranchFn<>((BranchFunction<C, S, E>) function));
        } else if (function instanceof InitialFunction) {
            fn = new InitialFn((InitialFunction<C, S>) function, traverserFactory);
        } else if (function instanceof FilterFunction) {
            fn = new FilterFn((FilterFunction<C, S>) function);
        } else if (function instanceof FlatMapFunction) {
            fn = new FlatMapFn<>((FlatMapFunction<C, S, E>) function);
        } else if (function instanceof MapFunction) {
            fn = new MapFn<>((MapFunction<C, S, E>) function);
        } else if (function instanceof ReduceFunction) {
            final ReduceFn<C, S, E> combine = new ReduceFn<>((ReduceFunction<C, S, E>) function, traverserFactory);
            collection = (PCollection<Traverser<C, ?>>) collection.apply(Combine.globally((ReduceFn) combine));
            this.functions.add(combine);
        } else
            throw new RuntimeException("You need a new step type:" + function);

        if (!(function instanceof ReduceFunction) && !(function instanceof BranchFunction)) {
            if (!branching)
                this.functions.add((Fn) fn);
            collection = (PCollection<Traverser<C, ?>>) collection.apply(ParDo.of((DoFn) fn));
        }
        collection.setCoder(new TraverserCoder());
        return collection;
    }

    @Override
    public void addStart(final Traverser<C, S> traverser) {
        this.functions.get(0).addStart(traverser);
    }

    @Override
    public Traverser<C, E> next() {
        this.setupPipeline();
        return this.iterator.next();
    }

    @Override
    public boolean hasNext() {
        this.setupPipeline();
        return this.iterator.hasNext();
    }

    @Override
    public void reset() {

    }

    @Override
    public String toString() {
        return this.functions.toString();
    }

    private final void setupPipeline() {
        if (null == this.iterator) {
            this.pipeline.run().waitUntilFinish();
            this.iterator = (Iterator) new ArrayList<>(OUTPUT).iterator();
            OUTPUT.clear();
        }
    }

}
