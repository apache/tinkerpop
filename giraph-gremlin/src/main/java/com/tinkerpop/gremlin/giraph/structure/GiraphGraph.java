package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.graph.map.GiraphGraphStep;
import com.tinkerpop.gremlin.giraph.process.olap.GiraphGraphComputer;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraph implements Graph {

    private Configuration configuration;

    private GiraphGraph() {

    }

    public static GiraphGraph open() {
        return GiraphGraph.open(Optional.empty());
    }

    public static <G extends Graph> G open(final Optional<Configuration> configuration) {
        final GiraphGraph graph = new GiraphGraph();
        graph.configuration = configuration.orElse(new BaseConfiguration());
        return (G) graph;
    }

    public GraphTraversal<Vertex, Vertex> V() {
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Vertex>();
        traversal.addStep(new GiraphGraphStep(traversal));
        return traversal;
    }

    public GraphTraversal<Edge, Edge> E() {
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Vertex>();
        traversal.addStep(new GiraphGraphStep(traversal));
        return traversal;
    }

    public Vertex v(final Object id) {
        throw new UnsupportedOperationException();
    }

    public Edge e(final Object id) {
        throw new UnsupportedOperationException();
    }

    public Vertex addVertex(final Object... keyValues) {
        throw new UnsupportedOperationException();
    }

    public <C extends GraphComputer> C compute(final Class<C>... graphComputerClass) {
        return (C) new GiraphGraphComputer().program(this.configuration);
    }


    public <V extends Variables> V variables() {
        return null;
    }

    public String toString() {
        return StringFactory.graphString(this, this.configuration.getString(GiraphGraphComputer.GREMLIN_INPUT_LOCATION) + "-" +
                this.configuration.getString(GiraphGraphComputer.VERTEX_PROGRAM) + "->" +
                this.configuration.getString(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION));
    }

    public void close() {

    }

    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }


    public Features getFeatures() {
        return null;
    }
}
