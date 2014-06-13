package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.graph.map.GiraphGraphStep;
import com.tinkerpop.gremlin.giraph.process.graph.strategy.SideEffectReplacementStrategy;
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
        return GiraphGraph.open(null);
    }

    public static <G extends Graph> G open(final Configuration configuration) {
        final GiraphGraph graph = new GiraphGraph();
        graph.configuration = Optional.ofNullable(configuration).orElse(new BaseConfiguration());
        return (G) graph;
    }

    public GraphTraversal<Vertex, Vertex> V() {
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Vertex>();
        traversal.strategies().register(new SideEffectReplacementStrategy());
        traversal.addStep(new GiraphGraphStep(traversal, Vertex.class));
        return traversal;
    }

    public GraphTraversal<Edge, Edge> E() {
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Vertex>();
        traversal.strategies().register(new SideEffectReplacementStrategy());
        traversal.addStep(new GiraphGraphStep(traversal, Edge.class));
        return traversal;
    }

    public Vertex v(final Object id) {
        throw Exceptions.vertexLookupsNotSupported();
    }

    public Edge e(final Object id) {
        throw Exceptions.edgeLookupsNotSupported();
    }

    public Vertex addVertex(final Object... keyValues) {
        throw Exceptions.vertexAdditionsNotSupported();
    }

    public <C extends GraphComputer> C compute(final Class<C>... graphComputerClass) {
        return (C) new GiraphGraphComputer(this, this.configuration);
    }


    public <V extends Variables> V variables() {
        throw Exceptions.variablesNotSupported();
    }

    public String toString() {
        return StringFactory.graphString(this,
                this.configuration.getString(GiraphGraphComputer.GIRAPH_VERTEX_INPUT_FORMAT_CLASS) +
                        " -> " +
                        this.configuration.getString(GiraphGraphComputer.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS));
    }

    public void close() {
        this.configuration = new BaseConfiguration();
    }

    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    public Features getFeatures() {
        return new Features() {
            @Override
            public GraphFeatures graph() {
                return new GraphFeatures() {
                    @Override
                    public boolean supportsComputer() {
                        return true;
                    }
                };
            }
        };
    }
}
