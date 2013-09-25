package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.ComputeResult;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.computer.GraphMemory;
import com.tinkerpop.blueprints.computer.GraphSystemMemory;
import com.tinkerpop.blueprints.computer.Isolation;
import com.tinkerpop.blueprints.computer.VertexMemory;
import com.tinkerpop.blueprints.computer.VertexSystemMemory;
import com.tinkerpop.blueprints.computer.util.CoreShellVertex;
import com.tinkerpop.blueprints.computer.util.DefaultGraphComputer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphComputer extends DefaultGraphComputer {

    private final TinkerGraph graph;
    private final GraphSystemMemory graphMemory = new TinkerGraphMemory();
    private VertexSystemMemory vertexMemory = new TinkerVertexMemory(this.isolation);

    int threads = Runtime.getRuntime().availableProcessors() - 1;
    int chunkSize = 1000;

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
            final ExecutorService executor = Executors.newFixedThreadPool(this.threads);
            final Iterator<Vertex> vertices = this.graph.query().vertices().iterator();
            while (vertices.hasNext()) {
                final Runnable worker = new VertexThread(vertices);
                executor.execute(worker);
            }
            executor.shutdown();
            while (!executor.isTerminated()) {
            }
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

    public class VertexThread implements Runnable {

        private final List<Vertex> vertices = new ArrayList<>();

        public VertexThread(final Iterator<Vertex> vertices) {
            for (int i = 0; i < chunkSize; i++) {
                if (!vertices.hasNext())
                    break;
                this.vertices.add(vertices.next());
            }
        }

        public void run() {
            final CoreShellVertex coreShellVertex = new CoreShellVertex(vertexMemory);
            for (final Vertex vertex : this.vertices) {
                //System.out.println(this + " " + vertex);
                coreShellVertex.setBaseVertex(vertex);
                vertexProgram.execute(coreShellVertex, graphMemory);
            }
        }

    }
}
