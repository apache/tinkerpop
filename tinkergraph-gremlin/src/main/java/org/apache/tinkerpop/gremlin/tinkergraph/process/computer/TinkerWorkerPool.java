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
package org.apache.tinkerpop.gremlin.tinkergraph.process.computer;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TinkerWorkerPool implements AutoCloseable {

    private static final BasicThreadFactory threadFactoryWorker = new BasicThreadFactory.Builder().namingPattern("tinker-worker-%d").build();
    public static enum State {VERTEX_PROGRAM, MAP_REDUCE}

    private final List<MapReduce> mapReducers;
    private final List<VertexProgram> vertexPrograms;
    private final State state;

    private final ExecutorService workerPool;

    public TinkerWorkerPool(final int numberOfWorkers, final VertexProgram vertexProgram) {
        try {
            this.state = State.VERTEX_PROGRAM;
            this.vertexPrograms = new ArrayList<>(numberOfWorkers);
            this.mapReducers = Collections.emptyList();
            for (int i = 0; i < numberOfWorkers; i++) {
                this.vertexPrograms.add(vertexProgram.clone());
            }

            workerPool = Executors.newFixedThreadPool(numberOfWorkers, threadFactoryWorker);

        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public TinkerWorkerPool(final int numberOfWorkers, final MapReduce mapReduce) {
        try {
            this.state = State.MAP_REDUCE;
            this.vertexPrograms = Collections.emptyList();
            this.mapReducers = new ArrayList<>(numberOfWorkers);
            for (int i = 0; i < numberOfWorkers; i++) {
                this.mapReducers.add(mapReduce.clone());
            }

            workerPool = Executors.newFixedThreadPool(numberOfWorkers, threadFactoryWorker);

        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void executeVertexProgram(final Consumer<VertexProgram> worker) {
        if (!this.state.equals(State.VERTEX_PROGRAM))
            throw new IllegalStateException("The provided TinkerWorkerPool is not setup for VertexProgram: " + this.state);

        this.vertexPrograms.stream()
                .map(vp -> (Future<Void>) workerPool.submit(() -> worker.accept(vp)))
                .collect(Collectors.toList())
                .forEach(FunctionUtils.wrapConsumer(Future::get));
    }

    public void executeMapReduce(final Consumer<MapReduce> worker) {
        if (!this.state.equals(State.MAP_REDUCE))
            throw new IllegalStateException("The provided TinkerWorkerPool is not setup for MapReduce: " + this.state);

        this.mapReducers.stream()
                .map(mr -> (Future<Void>) workerPool.submit(() -> worker.accept(mr)))
                .collect(Collectors.toList())
                .forEach(FunctionUtils.wrapConsumer(Future::get));
    }

    @Override
    public void close() throws Exception {
        workerPool.shutdown();
    }
}