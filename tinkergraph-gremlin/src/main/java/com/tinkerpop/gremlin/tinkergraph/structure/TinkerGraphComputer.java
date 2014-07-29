package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.SideEffects;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.tinkergraph.process.computer.TinkerMapEmitter;
import com.tinkerpop.gremlin.tinkergraph.process.computer.TinkerReduceEmitter;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphComputer implements GraphComputer {

    private Isolation isolation = Isolation.BSP;
    private VertexProgram vertexProgram;
    private final TinkerGraph graph;
    private final TinkerGraphComputerSideEffects sideEffects = new TinkerGraphComputerSideEffects();
    private final TinkerMessageBoard messageBoard = new TinkerMessageBoard();
    private boolean executed = false;
    private final List<MapReduce> mapReduces = new ArrayList<>();

    public TinkerGraphComputer(final TinkerGraph graph) {
        this.graph = graph;
    }

    public GraphComputer isolation(final Isolation isolation) {
        this.isolation = isolation;
        return this;
    }

    public GraphComputer program(final VertexProgram vertexProgram) {
        this.vertexProgram = vertexProgram;
        return this;
    }

    public GraphComputer mapReduce(final MapReduce mapReduce) {
        this.mapReduces.add(mapReduce);
        return this;
    }

    public Future<Pair<Graph, SideEffects>> submit() {
        if (this.executed)
            throw Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
        else
            this.executed = true;

        GraphComputerHelper.validateProgramOnComputer(this, vertexProgram);

        return CompletableFuture.<Pair<Graph, SideEffects>>supplyAsync(() -> {
            final long time = System.currentTimeMillis();

            final TinkerGraph g = this.graph;
            g.graphView = new TinkerGraphView(this.isolation, this.vertexProgram.getElementComputeKeys());
            g.useGraphView = true;
            // execute the vertex program
            this.vertexProgram.setup(this.sideEffects);

            while (true) {
                StreamFactory.parallelStream(g.V()).forEach(vertex -> this.vertexProgram.execute(vertex, new TinkerMessenger(vertex, this.messageBoard, this.vertexProgram.getMessageCombiner()), this.sideEffects));
                this.sideEffects.incrIteration();
                g.graphView.completeIteration();
                this.messageBoard.completeIteration();
                if (this.vertexProgram.terminate(this.sideEffects)) break;
            }

            this.mapReduces.addAll(this.vertexProgram.getMapReducers());
            for (final MapReduce mapReduce : this.mapReduces) {
                if (mapReduce.doStage(MapReduce.Stage.MAP)) {
                    final TinkerMapEmitter mapEmitter = new TinkerMapEmitter(mapReduce.doStage(MapReduce.Stage.REDUCE));
                    StreamFactory.parallelStream(g.V()).forEach(vertex -> mapReduce.map(vertex, mapEmitter));
                    // no need to run combiners as this is single machine
                    if (mapReduce.doStage(MapReduce.Stage.REDUCE)) {
                        final TinkerReduceEmitter reduceEmitter = new TinkerReduceEmitter();
                        mapEmitter.reduceMap.forEach((k, v) -> mapReduce.reduce(k, ((List) v).iterator(), reduceEmitter));
                        mapReduce.addToSideEffects(this.sideEffects, reduceEmitter.resultList.iterator());
                    } else {
                        mapReduce.addToSideEffects(this.sideEffects, mapEmitter.mapList.iterator());
                    }
                }
            }

            // update runtime and return the newly computed graph
            this.sideEffects.setRuntime(System.currentTimeMillis() - time);
            return new Pair<>(this.graph, this.sideEffects);
        });
    }

    public static void mergeComputedView(final Graph originalGraph, final Graph viewGraph, final Map<String, String> keyMapping) {
        if (originalGraph.getClass() != TinkerGraph.class)
            throw new IllegalArgumentException("The original graph provided is not a TinkerGraph: " + originalGraph.getClass());
        if (viewGraph.getClass() != TinkerGraph.class)
            throw new IllegalArgumentException("The computed graph provided is not a TinkerGraph: " + viewGraph.getClass());

        StreamFactory.parallelStream(viewGraph.V()).forEach(v1 -> {
            Vertex v2 = originalGraph.v(v1.id());
            keyMapping.forEach((key1, key2) -> {
                if (v1.property(key1).isPresent()) {
                    final Object value = v1.property(key1).value();
                    ((TinkerGraph) originalGraph).useGraphView = false;
                    v2.property(key2, value);
                    ((TinkerGraph) originalGraph).useGraphView = true;
                }
            });
        });
        TinkerHelper.dropView((TinkerGraph) originalGraph);
    }

}
