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
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.apache.tinkerpop.machine.coefficients.LongCoefficient;
import org.apache.tinkerpop.machine.functions.CFunction;
import org.apache.tinkerpop.machine.functions.FilterFunction;
import org.apache.tinkerpop.machine.functions.MapFunction;
import org.apache.tinkerpop.machine.functions.initial.InjectInitial;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.traversers.CompleteTraverser;
import org.apache.tinkerpop.machine.traversers.Traverser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Beam<C, S, E> implements Processor<C, S, E> {

    final Pipeline pipeline;
    PCollection collection;
    public static List<Traverser> OUTPUT = new ArrayList<>();
    Iterator<Traverser> iterator = null;

    public Beam(final Bytecode<C> bytecode) {
        this.pipeline = Pipeline.create();
        this.pipeline.getCoderRegistry().registerCoderForClass(Traverser.class, new TraverserCoder<>());

        for (final CFunction<?> function : BytecodeUtil.compile(bytecode)) {
            if (function instanceof InjectInitial) {
                final List<Traverser<C, S>> objects = new ArrayList<>();
                final Iterator<S> iterator = ((InjectInitial) function).get();
                while (iterator.hasNext())
                    objects.add(new CompleteTraverser(LongCoefficient.create(), iterator.next()));
                this.collection = this.pipeline.apply(Create.of(objects).withCoder(new TraverserCoder<>()));
            } else if (function instanceof FilterFunction) {
                collection = (PCollection) collection.apply(ParDo.of(new FilterFn<>((FilterFunction<C, S>) function)));
                collection.setCoder(new TraverserCoder());
            } else if (function instanceof MapFunction) {
                collection = (PCollection) collection.apply(ParDo.of(new MapFn<>((MapFunction<C, S, E>) function)));
                collection.setCoder(new TraverserCoder());
            } else
                throw new RuntimeException("You need a new step type:" + function);
        }
        collection = (PCollection) collection.apply(ParDo.of(new OutputStep()));

    }

    @Override
    public void addStart(Traverser<C, S> traverser) {

    }

    @Override
    public Traverser<C, E> next() {
        if (null == this.iterator) {
            pipeline.run().waitUntilFinish();
            this.iterator = OUTPUT.iterator();
        }
        return this.iterator.next();
    }

    @Override
    public boolean hasNext() {
        if (null == this.iterator) {
            pipeline.run().waitUntilFinish();
            this.iterator = OUTPUT.iterator();
        }
        return this.iterator.hasNext();
    }

    @Override
    public void reset() {

    }

    @Override
    public String toString() {
        return this.pipeline.toString();
    }
}
