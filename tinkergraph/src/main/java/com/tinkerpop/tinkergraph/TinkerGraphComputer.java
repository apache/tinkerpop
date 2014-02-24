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
            final TinkerGraph g = this.graph; //.cloneTinkerGraph();
            g.isolation = this.isolation;
            g.elementMemory = new TinkerElementMemory(this.isolation, this.vertexProgram.getComputeKeys());
            //g.memory = new TinkerGraphMemory(g);
            //g.memory.addAll(this.graph.memory);
            this.vertexProgram.setup(g.memory());

            while (true) {
                StreamFactory.parallelStream(g.V()).forEach(vertex ->
                        this.vertexProgram.execute(vertex, this.messenger, g.memory()));

                g.elementMemory.completeIteration();
                ((Graph.Memory.System) g.memory()).incrIteration();
                this.messenger.completeIteration();
                if (this.vertexProgram.terminate(g.memory())) break;
            }

            ((Graph.Memory.System) g.memory()).setRuntime(System.currentTimeMillis() - time);

            return g;
        });
    }

}
