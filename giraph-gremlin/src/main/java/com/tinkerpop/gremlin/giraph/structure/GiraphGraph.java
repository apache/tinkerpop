package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.graph.step.map.GiraphGraphStep;
import com.tinkerpop.gremlin.giraph.process.graph.strategy.SideEffectReplacementStrategy;
import com.tinkerpop.gremlin.giraph.process.graph.strategy.ValidateStepsStrategy;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.marker.TraverserSource;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;

import java.io.Serializable;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraph implements Graph, Serializable {

    private SConfiguration configuration;

    private GiraphGraph() {

    }

    public static GiraphGraph open() {
        return GiraphGraph.open(null);
    }

    public static <G extends Graph> G open(final Configuration configuration) {
        final GiraphGraph graph = new GiraphGraph();
        graph.configuration = new SConfiguration(Optional.ofNullable(configuration).orElse(new BaseConfiguration()));
        return (G) graph;
    }

    public GraphTraversal<Vertex, Vertex> V() {
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Vertex>() {
            public GraphTraversal<Object, Vertex> submit(final TraversalEngine engine) {
                if (engine instanceof GraphComputer) {
                    ((TraverserSource) this.getSteps().get(0)).clear();
                }
                return super.submit(engine);
            }
        };

        traversal.strategies().register(new SideEffectReplacementStrategy());
        traversal.strategies().register(new ValidateStepsStrategy());
        traversal.addStep(new GiraphGraphStep(traversal, Vertex.class, ConfUtil.makeHadoopConfiguration(this.configuration)));
        return traversal;
    }

    public GraphTraversal<Edge, Edge> E() {
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Vertex>();
        traversal.strategies().register(new SideEffectReplacementStrategy());
        traversal.strategies().register(new ValidateStepsStrategy());
        traversal.addStep(new GiraphGraphStep(traversal, Edge.class, ConfUtil.makeHadoopConfiguration(this.configuration)));
        return traversal;
    }

    /*public Vertex v(final Object id) {
        this.V().has(Element.ID,id).next()
    }*/

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
        final org.apache.hadoop.conf.Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(this.configuration);
        final String fromString = this.configuration.containsKey(GiraphGraphComputer.GIRAPH_VERTEX_INPUT_FORMAT_CLASS) ?
                hadoopConfiguration.getClass(GiraphGraphComputer.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class).getSimpleName() :
                "none";
        final String toString = this.configuration.containsKey(GiraphGraphComputer.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS) ?
                hadoopConfiguration.getClass(GiraphGraphComputer.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS, VertexOutputFormat.class).getSimpleName() :
                "none";
        return StringFactory.graphString(this, fromString.toLowerCase() + "->" + toString.toLowerCase());
    }

    public void close() {
        this.configuration.clear();
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
