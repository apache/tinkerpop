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
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.tinkerpop.machine.beam.serialization.TraverserCoder;
import org.apache.tinkerpop.machine.beam.util.PrimitiveVisitor;
import org.apache.tinkerpop.machine.beam.util.TopologyUtil;
import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.traversers.EmptyTraverser;
import org.apache.tinkerpop.machine.traversers.Traverser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Beam<C, S, E> implements Processor<C, S, E> {

    public static final int MAX_REPETIONS = 10;

    private final Pipeline pipeline;
    public static List<Traverser> OUTPUT = new ArrayList<>(); // FIX THIS!
    private final List<Fn> functions = new ArrayList<>();
    private Iterator<Traverser<C, E>> iterator = null;

    public Beam(final Compilation<C, S, E> compilation) {
        this.pipeline = Pipeline.create();
        this.pipeline.getOptions().setRunner(new PipelineOptions.DirectRunner().create(this.pipeline.getOptions()));
        final PCollection<Traverser<C, S>> source = this.pipeline.apply(Create.of(EmptyTraverser.instance()));
        source.setCoder(new TraverserCoder<>());
        final PCollection<Traverser<C, E>> sink = TopologyUtil.compile(source, compilation);
        sink.apply(ParDo.of(new OutputStep<>())); // TODO: we need an in-memory router of outgoing data

    }

    @Override
    public void addStart(final Traverser<C, S> traverser) {
        this.functions.get(0).addStart(traverser); // TODO: use side-inputs
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
        OUTPUT.clear();
        this.iterator = null;
    }

    @Override
    public String toString() {
        final PrimitiveVisitor visitor = new PrimitiveVisitor();
        this.pipeline.traverseTopologically(visitor);
        return visitor.toString();
    }

    private final void setupPipeline() {
        if (null == this.iterator) {
            this.pipeline.run().waitUntilFinish();
            this.iterator = (Iterator) new ArrayList<>(OUTPUT).iterator();
            OUTPUT.clear();
        }
    }

}
