package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.graph.step.map.GiraphGraphStep;
import com.tinkerpop.gremlin.giraph.process.graph.strategy.SideEffectReplacementStrategy;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
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

    protected SConfiguration configuration;

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
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Vertex>();
        traversal.strategies().register(new SideEffectReplacementStrategy());
        //traversal.strategies().register(new ValidateStepsStrategy());
        traversal.addStep(new GiraphGraphStep(traversal, Vertex.class, this));
        return traversal;
    }

    public GraphTraversal<Edge, Edge> E() {
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Vertex>();
        traversal.strategies().register(new SideEffectReplacementStrategy());
        //traversal.strategies().register(new ValidateStepsStrategy());
        traversal.addStep(new GiraphGraphStep(traversal, Edge.class, this));
        return traversal;
    }

    public Vertex v(final Object id) {
        return new GiraphVertex(this.V().<Vertex>has(Element.ID, id).next(), this);
    }

    public Edge e(final Object id) {
        return new GiraphEdge(this.E().<Edge>has(Element.ID, id).next(), this);
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

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public GiraphGraph getOutputGraph() {
        final Configuration conf = new BaseConfiguration();
        this.configuration.getKeys().forEachRemaining(key -> conf.setProperty(key, this.configuration.getString(key)));
        if (this.configuration.containsKey(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION)) {
            conf.setProperty(GiraphGraphComputer.GREMLIN_INPUT_LOCATION, this.configuration.getString(GiraphGraphComputer.GREMLIN_OUTPUT_LOCATION));
        }
        if (this.configuration.containsKey(GiraphGraphComputer.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS)) {
            conf.setProperty(GiraphGraphComputer.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, this.configuration.getString(GiraphGraphComputer.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS).replace("OutputFormat", "InputFormat"));
        }
        return GiraphGraph.open(conf);
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
