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

    private final int numberOfWorkers;
    private final ExecutorService workerPool;

    public TinkerWorkerPool(final int numberOfWorkers) {
        this.numberOfWorkers = numberOfWorkers;
        workerPool = Executors.newFixedThreadPool(numberOfWorkers, threadFactoryWorker);
    }

    public void executeVertexProgram(final Consumer<VertexProgram> worker, final VertexProgram vertexProgram) {
        final List<VertexProgram> vertexPrograms = cloneVertexProgram(vertexProgram);
        vertexPrograms.stream()
                .map(vp -> (Future<Void>) workerPool.submit(() -> worker.accept(vp)))
                .collect(Collectors.toList())
                .forEach(FunctionUtils.wrapConsumer(Future::get));
    }

    public void executeMapReduce(final Consumer<MapReduce> worker, final MapReduce mapReduce) {
        final List<MapReduce> mapReducers = cloneMapReducer(mapReduce);
        mapReducers.stream()
                .map(mr -> (Future<Void>) workerPool.submit(() -> worker.accept(mr)))
                .collect(Collectors.toList())
                .forEach(FunctionUtils.wrapConsumer(Future::get));
    }

    @Override
    public void close() throws Exception {
        workerPool.shutdown();
    }

    private List<VertexProgram> cloneVertexProgram(final VertexProgram vertexProgram) {
        final List<VertexProgram> vertexPrograms;
        vertexPrograms = new ArrayList<>(numberOfWorkers);
        for (int i = 0; i < numberOfWorkers; i++) {
            vertexPrograms.add(vertexProgram.clone());
        }
        return vertexPrograms;
    }

    private List<MapReduce> cloneMapReducer(final MapReduce mapReduce) {
        final List<MapReduce> mapReducers;
        mapReducers = new ArrayList<>(numberOfWorkers);
        for (int i = 0; i < numberOfWorkers; i++) {
            mapReducers.add(mapReduce.clone());
        }
        return mapReducers;
    }
}