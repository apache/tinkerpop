package com.tinkerpop.blueprints.tinkergraph;


import com.tinkerpop.blueprints.computer.ComputeResult;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.computer.GraphMemory;
import com.tinkerpop.blueprints.computer.VertexMemory;
import com.tinkerpop.blueprints.computer.VertexProgram;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphComputer implements GraphComputer {

    protected enum State {STANDARD, CENTRIC, ADJACENT}

    private Isolation isolation = Isolation.BSP;
    private VertexProgram vertexProgram;
    private final TinkerGraph graph;
    private final TinkerGraphMemory graphMemory = new TinkerGraphMemory();
    private final TinkerMessenger messenger = new TinkerMessenger();
    private TinkerVertexMemory vertexMemory = new TinkerVertexMemory(this.isolation);

    public TinkerGraphComputer(final TinkerGraph graph) {
        this.graph = graph;
    }

    public GraphComputer isolation(final Isolation isolation) {
        this.isolation = isolation;
        this.vertexMemory = new TinkerVertexMemory(isolation);
        return this;
    }

    public GraphComputer program(final VertexProgram program) {
        this.vertexProgram = program;
        return this;
    }

    public Future<ComputeResult> submit() {
        return CompletableFuture.<ComputeResult>supplyAsync(() -> {
            final long time = System.currentTimeMillis();
            this.vertexMemory.setComputeKeys(this.vertexProgram.getComputeKeys());
            this.vertexProgram.setup(this.graphMemory);

            boolean done = false;
            while (!done) {
                StreamFactory.parallelStream(this.graph.query().vertices()).forEach(vertex ->
                        this.vertexProgram.execute(((TinkerVertex) vertex).createClone(State.CENTRIC,
                                vertex.getId().toString(),
                                this.vertexMemory), this.messenger, this.graphMemory));

                this.vertexMemory.completeIteration();
                this.messenger.completeIteration();
                this.graphMemory.incrIteration();
                done = this.vertexProgram.terminate(this.graphMemory);
            }

            this.graphMemory.setRuntime(System.currentTimeMillis() - time);

            return new ComputeResult() {
                @Override
                public GraphMemory getGraphMemory() {
                    return graphMemory;
                }

                @Override
                public VertexMemory getVertexMemory() {
                    return vertexMemory;
                }
            };
        });
    }
}
