package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalResult;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.GraphMigrator;
import com.tinkerpop.gremlin.tinkergraph.process.graph.map.TinkerGraphStep;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphComputer implements GraphComputer, TraversalEngine {

    public static final String CLONE_GRAPH = "tinkergraph.computer.clone-graph";

    private Isolation isolation = Isolation.BSP;
    private Configuration configuration = new BaseConfiguration();
    private final TinkerGraph graph;
    private final TinkerMessenger messenger = new TinkerMessenger();

    public TinkerGraphComputer(final TinkerGraph graph) {
        this.graph = graph;
    }

    public <E> Iterator<E> execute(final Traversal<?, E> traversal) {
        ((TinkerGraphStep) traversal.getSteps().get(0)).clear();
        return new TraversalResult<>(this.graph, () -> traversal);
    }

    public GraphComputer isolation(final Isolation isolation) {
        this.isolation = isolation;
        return this;
    }

    public GraphComputer program(final Configuration configuration) {
        configuration.getKeys().forEachRemaining(key -> this.configuration.setProperty(key, configuration.getProperty(key)));
        return this;
    }

    public Future<Graph> submit() {
        final VertexProgram vertexProgram = VertexProgram.createVertexProgram(this.configuration);
        GraphComputerHelper.validateProgramOnComputer(this, vertexProgram);

        return CompletableFuture.<Graph>supplyAsync(() -> {
            final long time = System.currentTimeMillis();

            // clone the graph or operate directly on the existing graph
            final TinkerGraph g;
            if (this.configuration.getBoolean(CLONE_GRAPH, false)) {
                try {
                    g = TinkerGraph.open();
                    GraphMigrator.migrateGraph(this.graph, g);
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            } else {
                g = this.graph;
            }


            g.usesElementMemory = true;
            g.elementMemory = new TinkerElementMemory(this.isolation, vertexProgram.getComputeKeys());


            // execute the vertex program
            vertexProgram.setup(g.memory());
            while (true) {
                StreamFactory.parallelStream(g.V()).forEach(vertex -> vertexProgram.execute(vertex, this.messenger, g.memory()));
                g.<Graph.Memory.Computer.Administrative>memory().incrIteration();
                g.elementMemory.completeIteration();
                this.messenger.completeIteration();
                if (vertexProgram.terminate(g.memory())) break;
            }

            // update runtime and return the newly computed graph
            g.<Graph.Memory.Computer.Administrative>memory().setRuntime(System.currentTimeMillis() - time);
            return g;
        });
    }

}
