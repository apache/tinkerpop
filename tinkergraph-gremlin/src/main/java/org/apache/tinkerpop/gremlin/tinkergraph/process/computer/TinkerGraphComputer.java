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

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TinkerGraphComputer implements GraphComputer {

    private Isolation isolation = Isolation.BSP;
    private Optional<ResultGraph> resultGraph = Optional.empty();
    private Optional<Persist> persist = Optional.empty();

    private VertexProgram<?> vertexProgram;
    private final TinkerGraph graph;
    private TinkerMemory memory;
    private final TinkerMessageBoard messageBoard = new TinkerMessageBoard();
    private boolean executed = false;
    private final Set<MapReduce> mapReducers = new HashSet<>();


    public TinkerGraphComputer(final TinkerGraph graph) {
        this.graph = graph;
    }

    public static TraversalEngine engine() {
        return null;
    }

    @Override
    public GraphComputer isolation(final Isolation isolation) {
        this.isolation = isolation;
        return this;
    }

    @Override
    public GraphComputer result(final ResultGraph resultGraph) {
        this.resultGraph = Optional.of(resultGraph);
        return this;
    }

    @Override
    public GraphComputer persist(final Persist persist) {
        this.persist = Optional.of(persist);
        return this;
    }

    @Override
    public GraphComputer program(final VertexProgram vertexProgram) {
        this.vertexProgram = vertexProgram;
        return this;
    }

    @Override
    public GraphComputer mapReduce(final MapReduce mapReduce) {
        this.mapReducers.add(mapReduce);
        return this;
    }

    @Override
    public Future<ComputerResult> submit() {
        if (this.executed)
            throw Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
        else
            this.executed = true;

        // it is not possible execute a computer if it has no vertex program nor mapreducers
        if (null == this.vertexProgram && this.mapReducers.isEmpty())
            throw GraphComputer.Exceptions.computerHasNoVertexProgramNorMapReducers();
        // it is possible to run mapreducers without a vertex program
        if (null != this.vertexProgram) {
            GraphComputerHelper.validateProgramOnComputer(this, this.vertexProgram);
            this.mapReducers.addAll(this.vertexProgram.getMapReducers());
        }

        if (!this.persist.isPresent())
            this.persist = Optional.of(null == this.vertexProgram ? Persist.NOTHING : this.vertexProgram.getPreferredPersist());
        if (!this.resultGraph.isPresent())
            this.resultGraph = Optional.of(null == this.vertexProgram ? ResultGraph.ORIGINAL : this.vertexProgram.getPreferredResultGraph());
        if (this.resultGraph.get().equals(ResultGraph.ORIGINAL))
            if (!this.persist.get().equals(Persist.NOTHING))
                throw GraphComputer.Exceptions.resultGraphPersistCombinationNotSupported(this.resultGraph.get(), this.persist.get());

        //final Graph computeGraph = this.graph;
        final ComputerGraph computeGraph = new ComputerGraph(this.graph, null == this.vertexProgram ? Collections.emptySet() : this.vertexProgram.getElementComputeKeys());
        this.memory = new TinkerMemory(this.vertexProgram, this.mapReducers);
        return CompletableFuture.<ComputerResult>supplyAsync(() -> {
            final long time = System.currentTimeMillis();
            try (final TinkerWorkerPool workers = new TinkerWorkerPool(Runtime.getRuntime().availableProcessors())) {
                if (null != this.vertexProgram) {
                    TinkerHelper.createGraphView(this.graph, this.isolation, this.vertexProgram.getElementComputeKeys());
                    computeGraph.setState(ComputerGraph.State.VERTEX_PROGRAM);
                    // execute the vertex program
                    this.vertexProgram.setup(this.memory);
                    this.memory.completeSubRound();
                    while (true) {
                        workers.setVertexProgram(this.vertexProgram);
                        workers.vertexProgramWorkerIterationStart(this.memory.asImmutable());
                        final SynchronizedIterator<Vertex> vertices = new SynchronizedIterator<>(computeGraph.vertices());
                        workers.executeVertexProgram(vertexProgram -> {
                            while (true) {
                                final Vertex vertex = vertices.next();
                                if (null == vertex) return;
                                vertexProgram.execute(vertex, new TinkerMessenger<>(vertex, this.messageBoard, vertexProgram.getMessageCombiner()), this.memory);
                            }
                        });
                        workers.vertexProgramWorkerIterationEnd(this.memory.asImmutable());
                        this.messageBoard.completeIteration();
                        this.memory.completeSubRound();
                        if (this.vertexProgram.terminate(this.memory)) {
                            this.memory.incrIteration();
                            this.memory.completeSubRound();
                            break;
                        } else {
                            this.memory.incrIteration();
                            this.memory.completeSubRound();
                        }
                    }
                }

                // execute mapreduce jobs
                computeGraph.setState(ComputerGraph.State.MAP_REDUCE);
                for (final MapReduce mapReduce : mapReducers) {
                    if (mapReduce.doStage(MapReduce.Stage.MAP)) {
                        final TinkerMapEmitter<?, ?> mapEmitter = new TinkerMapEmitter<>(mapReduce.doStage(MapReduce.Stage.REDUCE));
                        final SynchronizedIterator<Vertex> vertices = new SynchronizedIterator<>(computeGraph.vertices());
                        workers.setMapReduce(mapReduce);
                        workers.mapReduceWorkerStart(MapReduce.Stage.MAP);
                        workers.executeMapReduce(workerMapReduce -> {
                            while (true) {
                                final Vertex vertex = vertices.next();
                                if (null == vertex) return;
                                workerMapReduce.map(vertex, mapEmitter);
                            }
                        });
                        workers.mapReduceWorkerEnd(MapReduce.Stage.MAP);

                        // sort results if a map output sort is defined
                        mapEmitter.complete(mapReduce);

                        // no need to run combiners as this is single machine
                        if (mapReduce.doStage(MapReduce.Stage.REDUCE)) {
                            final TinkerReduceEmitter<?, ?> reduceEmitter = new TinkerReduceEmitter<>();
                            final SynchronizedIterator<Map.Entry<?, Queue<?>>> keyValues = new SynchronizedIterator((Iterator) mapEmitter.reduceMap.entrySet().iterator());
                            workers.mapReduceWorkerStart(MapReduce.Stage.REDUCE);
                            workers.executeMapReduce(workerMapReduce -> {
                                while (true) {
                                    final Map.Entry<?, Queue<?>> entry = keyValues.next();
                                    if (null == entry) return;
                                    workerMapReduce.reduce(entry.getKey(), entry.getValue().iterator(), reduceEmitter);
                                }
                            });
                            workers.mapReduceWorkerEnd(MapReduce.Stage.REDUCE);
                            reduceEmitter.complete(mapReduce); // sort results if a reduce output sort is defined
                            mapReduce.addResultToMemory(this.memory, reduceEmitter.reduceQueue.iterator());
                        } else {
                            mapReduce.addResultToMemory(this.memory, mapEmitter.mapQueue.iterator());
                        }
                    }
                }
                // update runtime and return the newly computed graph
                this.memory.setRuntime(System.currentTimeMillis() - time);
                this.memory.complete();
                return new TinkerComputerResult(this.graph, this.memory.asImmutable());

            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    @Override
    public String toString() {
        return StringFactory.graphComputerString(this);
    }

    private static class SynchronizedIterator<V> {

        private final Iterator<V> iterator;

        public SynchronizedIterator(final Iterator<V> iterator) {
            this.iterator = iterator;
        }

        public synchronized V next() {
            return this.iterator.hasNext() ? this.iterator.next() : null;
        }
    }
}