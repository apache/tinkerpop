package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalResult;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.process.util.HolderSource;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphComputer implements GraphComputer, TraversalEngine {

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
        ((HolderSource) traversal.getSteps().get(0)).clear();
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

            final TinkerGraph g = this.graph;
            g.graphView = new TinkerGraphView(this.isolation, vertexProgram.getComputeKeys());
            g.useGraphView = true;
            // execute the vertex program
            vertexProgram.setup(this.sideEffects);
            while (true) {
                StreamFactory.parallelStream(g.V()).forEach(vertex -> vertexProgram.execute(vertex, this.messenger, this.sideEffects));
                this.sideEffects.incrIteration();
                g.graphView.completeIteration();
                this.messenger.completeIteration();
                if (vertexProgram.terminate(this.sideEffects)) break;
            }

            // update runtime and return the newly computed graph
            this.sideEffects.setRuntime(System.currentTimeMillis() - time);
            return new Pair<Graph, SideEffects>(this.graph, this.sideEffects);
        });
    }

    public static void mergeComputedView(final Graph originalGraph, final Graph viewGraph, final Map<String, String> keyMapping) {
        if (originalGraph.getClass() != TinkerGraph.class)
            throw new IllegalArgumentException("The original graph provided is not a TinkerGraph: " + originalGraph.getClass());
        if (viewGraph.getClass() != TinkerGraph.class)
            throw new IllegalArgumentException("The computed graph provided is not a TinkerGraph: " + viewGraph.getClass());

        StreamFactory.parallelStream(viewGraph.V()).forEach(v1 -> {
            Vertex v2 = originalGraph.v(v1.id());
            keyMapping.forEach((key1, key2) -> {
                if (v1.property(key1).isPresent()) {
                    final Object value = v1.property(key1).get();
                    ((TinkerGraph) originalGraph).useGraphView = false;
                    v2.property(key2, value);
                    ((TinkerGraph) originalGraph).useGraphView = true;
                }
            });
        });
        TinkerHelper.dropView((TinkerGraph) originalGraph);
    }

}
