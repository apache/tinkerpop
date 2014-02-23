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
            final long time = java.lang.System.currentTimeMillis();
            //this.vertexMemory.setComputeKeys(this.vertexProgram.getComputeKeys());
            this.vertexProgram.setup(this.graph.memory());

            while (true) {
                StreamFactory.parallelStream(this.graph.V()).forEach(vertex ->
                        /*this.vertexProgram.execute(((TinkerVertex) vertex).createClone(State.CENTRIC,
                                vertex.getId().toString(),
                                this.vertexMemory), this.messenger, this.graphMemory));*/
                        this.vertexProgram.execute(vertex, this.messenger, this.graph.memory()));

                //this.vertexMemory.completeIteration();
                ((Graph.Memory.System) this.graph.memory()).incrIteration();
                this.messenger.completeIteration();
                if (this.vertexProgram.terminate(this.graph.memory())) break;
            }

            ((Graph.Memory.System) this.graph.memory()).setRuntime(java.lang.System.currentTimeMillis() - time);

            return this.graph;
        });
    }
}
