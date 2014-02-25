package com.tinkerpop.tinkergraph;


import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.olap.GraphComputer;
import com.tinkerpop.gremlin.process.olap.VertexProgram;
import com.tinkerpop.gremlin.process.olap.traversal.TraversalResult;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphComputer implements GraphComputer, TraversalEngine {

    private Isolation isolation = Isolation.BSP;
    private VertexProgram vertexProgram;
    private final TinkerGraph graph;
    private final TinkerMessenger messenger = new TinkerMessenger();

    public TinkerGraphComputer(final TinkerGraph graph) {
        this.graph = graph;
    }

    public <E> Iterator<E> execute(final Traversal<?, E> traversal) {
        return new TraversalResult<>(this.graph, () -> traversal);
    }

    public GraphComputer isolation(final Isolation isolation) {
        this.isolation = isolation;
        return this;
    }

    public GraphComputer program(final VertexProgram program) {
        this.vertexProgram = program;
        return this;
    }

    public Future<Graph> submit() {
        return CompletableFuture.<Graph>supplyAsync(() -> {
            final long time = System.currentTimeMillis();

            // clone the graph
            final TinkerGraph g = TinkerHelper.cloneTinkerGraph(this.graph);
            g.isolation = this.isolation;
            g.elementMemory = new TinkerElementMemory(this.isolation, this.vertexProgram.getComputeKeys());
            g.graphMemory = new TinkerGraphMemory(g);
            g.graphMemory.addAll(this.graph.graphMemory);

            // execute the vertex program
            this.vertexProgram.setup(g.memory());
            while (true) {
                StreamFactory.parallelStream(g.V()).forEach(vertex -> this.vertexProgram.execute(vertex, this.messenger, g.memory()));
                g.<Graph.Memory.Computer.System>memory().incrIteration();
                g.elementMemory.completeIteration();
                this.messenger.completeIteration();
                if (this.vertexProgram.terminate(g.memory())) break;
            }

            // update runtime and return the newly computed graph
            g.<Graph.Memory.Computer.System>memory().setRuntime(System.currentTimeMillis() - time);
            return g;
        });
    }

}
