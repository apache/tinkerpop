package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.computer.ComputeResult;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.computer.GraphMemory;
import com.tinkerpop.blueprints.computer.GraphSystemMemory;
import com.tinkerpop.blueprints.computer.Isolation;
import com.tinkerpop.blueprints.computer.VertexMemory;
import com.tinkerpop.blueprints.computer.VertexSystemMemory;
import com.tinkerpop.blueprints.computer.util.CoreShellVertex;
import com.tinkerpop.blueprints.computer.util.DefaultGraphComputer;
import com.tinkerpop.blueprints.util.StreamFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphComputer extends DefaultGraphComputer {

    private final TinkerGraph graph;
    private final GraphSystemMemory graphMemory = new TinkerGraphMemory();
    private VertexSystemMemory vertexMemory = new TinkerVertexMemory(this.isolation);

    public TinkerGraphComputer(final TinkerGraph graph) {
        this.graph = graph;
    }

    public GraphComputer isolation(final Isolation isolation) {
        super.isolation(isolation);
        this.vertexMemory = new TinkerVertexMemory(isolation);
        return this;
    }

    public ComputeResult submit() {
        final long time = System.currentTimeMillis();
        this.vertexMemory.setComputeKeys(this.vertexProgram.getComputeKeys());
        this.vertexProgram.setup(this.graphMemory);

        boolean done = false;
        while (!done) {
            StreamFactory.parallelStream(this.graph.query().vertices()).forEach(vertex -> {
                final CoreShellVertex coreShellVertex = new CoreShellVertex(vertexMemory);
                coreShellVertex.setBaseVertex(vertex);
                vertexProgram.execute(coreShellVertex, graphMemory);
            });

            this.vertexMemory.completeIteration();
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
    }
}
