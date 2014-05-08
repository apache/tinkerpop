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
import org.javatuples.Pair;

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
    private final TinkerGraphComputerSideEffects sideEffects = new TinkerGraphComputerSideEffects();
    private boolean executed = false;

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

    public Future<Pair<Graph, SideEffects>> submit() {
        if (this.executed)
            throw Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
        else
            this.executed = true;

        final VertexProgram vertexProgram = VertexProgram.createVertexProgram(this.configuration);
        GraphComputerHelper.validateProgramOnComputer(this, vertexProgram);

        return CompletableFuture.<Pair<Graph, SideEffects>>supplyAsync(() -> {
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
            vertexProgram.setup(this.sideEffects);
            while (true) {
                StreamFactory.parallelStream(g.V()).forEach(vertex -> vertexProgram.execute(vertex, this.messenger, this.sideEffects));
                this.sideEffects.incrIteration();
                g.elementMemory.completeIteration();
                this.messenger.completeIteration();
                if (vertexProgram.terminate(this.sideEffects)) break;
            }

            // update runtime and return the newly computed graph
            this.sideEffects.setRuntime(System.currentTimeMillis() - time);
            return new Pair<Graph, SideEffects>(g, this.sideEffects);
        });
    }

}
