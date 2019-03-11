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
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.apache.tinkerpop.machine.coefficients.Coefficient;
import org.apache.tinkerpop.machine.coefficients.LongCoefficient;
import org.apache.tinkerpop.machine.functions.CFunction;
import org.apache.tinkerpop.machine.functions.FilterFunction;
import org.apache.tinkerpop.machine.functions.InitialFunction;
import org.apache.tinkerpop.machine.functions.MapFunction;
import org.apache.tinkerpop.machine.functions.ReduceFunction;
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
    Iterator<Traverser> iterator = null;


    public Beam(final List<CFunction<C>> functions, final TraverserFactory<C> traverserFactory) {
        this.pipeline = Pipeline.create();
        this.pipeline.getCoderRegistry().registerCoderForClass(Traverser.class, new TraverserCoder<>());
        PCollection collection = this.pipeline.apply(Create.of(traverserFactory.create((Coefficient) LongCoefficient.create(), 1L)));
        collection.setCoder(new TraverserCoder());

        DoFn fn = null;
        for (final CFunction<?> function : functions) {
            if (function instanceof InitialFunction) {
                fn = new InitialFn<>((InitialFunction) function, traverserFactory);
            } else if (function instanceof FilterFunction) {
                fn = new FilterFn<>((FilterFunction) function);
            } else if (function instanceof MapFunction) {
                fn = new MapFn<>((MapFunction) function);
            } else if (function instanceof ReduceFunction) {
                final ReduceFn combine = new ReduceFn<>((ReduceFunction) function, traverserFactory);
                collection = (PCollection) collection.apply(Combine.globally(combine));
                this.functions.add(combine);
            } else
                throw new RuntimeException("You need a new step type:" + function);

            if (!(function instanceof ReduceFunction)) {
                this.functions.add((Fn) fn);
                collection = (PCollection) collection.apply(ParDo.of(fn));
            }
            collection.setCoder(new TraverserCoder());


        }
        collection.apply(ParDo.of(new OutputStep()));
        this.pipeline.getOptions().setRunner(new PipelineOptions.DirectRunner().create(this.pipeline.getOptions()));
    }

    public Beam(final Bytecode<C> bytecode) {
        this(BytecodeUtil.compile(BytecodeUtil.strategize(bytecode)), BytecodeUtil.getTraverserFactory(bytecode).get());
    }

    @Override
    public void addStart(Traverser<C, S> traverser) {
        ((Fn) this.functions.get(0)).addStart(traverser);
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
            this.iterator = new ArrayList<>(OUTPUT).iterator();
            OUTPUT.clear();
        }
    }

}
