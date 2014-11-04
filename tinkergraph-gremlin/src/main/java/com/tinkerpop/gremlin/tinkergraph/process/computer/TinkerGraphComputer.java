package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphComputer implements GraphComputer {

    private Isolation isolation = Isolation.BSP;
    private VertexProgram vertexProgram;
    private final TinkerGraph graph;
    private TinkerMemory memory;
    private final TinkerMessageBoard messageBoard = new TinkerMessageBoard();
    private boolean executed = false;
    private final Set<MapReduce> mapReduces = new HashSet<>();

    public TinkerGraphComputer(final TinkerGraph graph) {
        this.graph = graph;
    }

    @Override
    public GraphComputer isolation(final Isolation isolation) {
        this.isolation = isolation;
        return this;
    }

    @Override
    public GraphComputer program(final VertexProgram vertexProgram) {
        this.vertexProgram = vertexProgram;
        return this;
    }

    @Override
    public GraphComputer mapReduce(final MapReduce mapReduce) {
        this.mapReduces.add(mapReduce);
        return this;
    }

    @Override
    public Future<ComputerResult> submit() {
        if (this.executed)
            throw Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
        else
            this.executed = true;

        // it is not possible execute a computer if it has no vertex program nor mapreducers
        if (null == this.vertexProgram && this.mapReduces.isEmpty())
            throw GraphComputer.Exceptions.computerHasNoVertexProgramNorMapReducers();
        // it is possible to run mapreducers without a vertex program
        if (null != this.vertexProgram) {
            GraphComputerHelper.validateProgramOnComputer(this, this.vertexProgram);
            this.mapReduces.addAll(this.vertexProgram.getMapReducers());
        }

        this.memory = new TinkerMemory(this.vertexProgram, this.mapReduces);
        return CompletableFuture.<ComputerResult>supplyAsync(() -> {
            final long time = System.currentTimeMillis();
            if (null != this.vertexProgram) {
                TinkerHelper.createGraphView(this.graph, this.isolation, this.vertexProgram.getElementComputeKeys());
                // execute the vertex program
                this.vertexProgram.setup(this.memory);
                this.memory.completeSubRound();
                while (true) {
                    this.vertexProgram.workerIterationStart(this.memory);
                    TinkerHelper.getVertices(this.graph).stream().forEach(vertex ->
                            this.vertexProgram.execute(vertex, new TinkerMessenger(vertex, this.messageBoard), this.memory));
                    this.vertexProgram.workerIterationEnd(this.memory);
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
            for (final MapReduce mapReduce : this.mapReduces) {
                if (mapReduce.doStage(MapReduce.Stage.MAP)) {
                    final TinkerMapEmitter<?, ?> mapEmitter = new TinkerMapEmitter<>(mapReduce.doStage(MapReduce.Stage.REDUCE));
                    TinkerHelper.getVertices(this.graph).parallelStream().forEach(vertex -> mapReduce.map(vertex, mapEmitter));
                    mapEmitter.complete(mapReduce); // sort results if a map output sort is defined
                    // no need to run combiners as this is single machine
                    if (mapReduce.doStage(MapReduce.Stage.REDUCE)) {
                        final TinkerReduceEmitter<?, ?> reduceEmitter = new TinkerReduceEmitter<>();
                        mapEmitter.reduceMap.entrySet().parallelStream().forEach(entry -> mapReduce.reduce(entry.getKey(), entry.getValue().iterator(), reduceEmitter));
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
            return new ComputerResult(this.graph, this.memory);
        });
    }

    @Override
    public String toString() {
        return StringFactory.graphComputerString(this);
    }

}
