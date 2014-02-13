package com.tinkerpop.tinkergraph;


import com.tinkerpop.gremlin.process.olap.ComputeResult;
import com.tinkerpop.gremlin.process.olap.GraphComputer;
import com.tinkerpop.gremlin.process.olap.GraphMemory;
import com.tinkerpop.gremlin.process.olap.VertexMemory;
import com.tinkerpop.gremlin.process.olap.VertexProgram;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StreamFactory;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphComputer implements GraphComputer {

    protected enum State {STANDARD, CENTRIC, ADJACENT}

    private Isolation isolation = Isolation.BSP;
    private VertexProgram vertexProgram;
    private final TinkerGraph graph;
    private final TinkerGraphMemory graphMemory;
    private final TinkerMessenger messenger = new TinkerMessenger();
    private TinkerVertexMemory vertexMemory = new TinkerVertexMemory(this.isolation);

    public TinkerGraphComputer(final TinkerGraph graph) {
        this.graph = graph;
        this.graphMemory = new TinkerGraphMemory(graph);
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

    public <K, V, R> GraphComputer reduction(final BiFunction<K, Iterator<V>, R> reduction) {
        this.graphMemory.reductionMemory = new TinkerReductionMemory<>(reduction);
        return this;
    }

    public Future<ComputeResult> submit() {
        return CompletableFuture.<ComputeResult>supplyAsync(() -> {
            final long time = System.currentTimeMillis();
            this.vertexMemory.setComputeKeys(this.vertexProgram.getComputeKeys());
            this.vertexProgram.setup(this.graphMemory);

            while (true) {
                StreamFactory.parallelStream((Iterator<Vertex>) this.graph.V()).forEach(vertex ->
                        this.vertexProgram.execute(((TinkerVertex) vertex).createClone(State.CENTRIC,
                                vertex.getId().toString(),
                                this.vertexMemory), this.messenger, this.graphMemory));

                this.vertexMemory.completeIteration();
                this.graphMemory.incrIteration();
                this.messenger.completeIteration();
                if (this.vertexProgram.terminate(this.graphMemory)) break;
            }

            if (null != this.graphMemory.getReductionMemory())
                this.graphMemory.getReductionMemory().reduce();

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
