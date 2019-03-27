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
package org.apache.tinkerpop.machine.processor.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.processor.beam.io.TraverserCoder;
import org.apache.tinkerpop.machine.processor.beam.util.ExecutionPlanner;
import org.apache.tinkerpop.machine.processor.beam.util.TopologyUtil;
import org.apache.tinkerpop.machine.species.remote.TraverserServer;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.species.EmptyTraverser;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Beam<C, S, E> implements Processor<C, S, E> {

    public static final int MAX_REPETIONS = 15; // TODO: this needs to be a dynamic configuration
    private boolean createTraverserServer;
    private final int traverserServerPort;
    private final Pipeline pipeline;
    private PipelineResult pipelineResult;
    private Iterator<Traverser<C, E>> iterator = null;


    public Beam(final Compilation<C, S, E> compilation, final String traverserServerLocation, final int traverserServerPort, final boolean createTraverserServer) {
        this.traverserServerPort = traverserServerPort;
        this.createTraverserServer = createTraverserServer;
        ///
        this.pipeline = Pipeline.create();
        this.pipeline.getOptions().setRunner(new PipelineOptions.DirectRunner().create(this.pipeline.getOptions()));
        final PCollection<Traverser<C, S>> source = this.pipeline.apply(Create.of(EmptyTraverser.instance()));
        source.setCoder(new TraverserCoder<>());
        final PCollection<Traverser<C, E>> sink = TopologyUtil.compile(source, compilation);
        sink.apply(ParDo.of(new OutputFn<>(traverserServerLocation, this.traverserServerPort)));
    }

    @Override
    public void addStart(final Traverser<C, S> traverser) {
        // TODO: use side-inputs
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
        this.iterator = null;
    }

    @Override
    public String toString() {
        final ExecutionPlanner visitor = new ExecutionPlanner();
        this.pipeline.traverseTopologically(visitor);
        return visitor.toString();
    }

    private void setupPipeline() {
        if (null == this.iterator) {
            if (this.createTraverserServer) {
                this.iterator = new TraverserServer<>(this.traverserServerPort);
                this.pipelineResult = this.pipeline.run();
            } else {
                this.iterator = Collections.emptyIterator();
                this.pipeline.run().waitUntilFinish();
            }
        }
        if (this.createTraverserServer && this.pipelineResult.getState().isTerminal())
            ((TraverserServer) this.iterator).close();
    }
}
