package com.tinkerpop.gremlin.structure.util.subgraph;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.function.Function;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class Subgraph implements Graph {

    private final Graph baseGraph;
    private final Function<Vertex, Boolean> vertexCriterion;
    private final Function<Edge, Boolean> edgeCriterion;

    public Subgraph(final Graph wrappedGraph,
                    final Function<Vertex, Boolean> vertexCriterion,
                    final Function<Edge, Boolean> edgeCriterion) {
        this.baseGraph = wrappedGraph;
        this.vertexCriterion = vertexCriterion;
        this.edgeCriterion = edgeCriterion;
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        // note: created vertex may not pass the vertexCriterion
        return new SubgraphVertex(baseGraph.addVertex(keyValues), vertexCriterion, edgeCriterion);
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V() {
        return new SubgraphTraversal<>(baseGraph.V(), vertexCriterion, edgeCriterion, true, true);
    }

    @Override
    public GraphTraversal<Edge, Edge> E() {
        return new SubgraphTraversal(baseGraph.E(), vertexCriterion, edgeCriterion, false, false);
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C>... graphComputerClass) {
        // TODO: wrap this?
        return baseGraph.compute(graphComputerClass);
    }

    @Override
    public Transaction tx() {
        return baseGraph.tx();
    }

    @Override
    public <V extends Variables> V variables() {
        return baseGraph.variables();
    }

    @Override
    public void close() throws Exception {
        baseGraph.close();
    }
}
