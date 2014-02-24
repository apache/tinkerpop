package com.tinkerpop.tinkergraph;


import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.olap.GraphComputer;
import com.tinkerpop.gremlin.process.olap.VertexProgram;
import com.tinkerpop.gremlin.process.olap.traversal.TraversalResult;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphComputer implements GraphComputer, TraversalEngine {

    private Isolation isolation = Isolation.BSP;
    private String jobId = UUID.randomUUID().toString();
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

    public GraphComputer jobId(final String jobId) {
        this.jobId = jobId;
        return this;
    }

    public Future<Graph> submit() {
        return CompletableFuture.<Graph>supplyAsync(() -> {
            final long time = java.lang.System.currentTimeMillis();
            //this.vertexMemory.setComputeKeys(this.vertexProgram.getComputeKeys());
            this.graph.memories.put(this.jobId, new TinkerGraphMemory(this.graph));
            this.vertexProgram.setup(this.graph.memory(this.jobId));

            while (true) {
                StreamFactory.parallelStream(this.graph.V()).forEach(vertex ->
                        /*this.vertexProgram.execute(((TinkerVertex) vertex).createClone(State.CENTRIC,
                                vertex.getId().toString(),
                                this.vertexMemory), this.messenger, this.graphMemory));*/
                        this.vertexProgram.execute(vertex, this.messenger, this.graph.memory(this.jobId)));

                //this.vertexMemory.completeIteration();
                ((Graph.Memory.System) this.graph.memory(this.jobId)).incrIteration();
                this.messenger.completeIteration();
                if (this.vertexProgram.terminate(this.graph.memory(this.jobId))) break;
            }

            ((Graph.Memory.System) this.graph.memory(this.jobId)).setRuntime(java.lang.System.currentTimeMillis() - time);

            return this.graph;
        });
    }
}
