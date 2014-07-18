package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.computer.util.GiraphComputerHelper;
import com.tinkerpop.gremlin.giraph.process.graph.step.map.GiraphGraphStep;
import com.tinkerpop.gremlin.process.TraversalEngine;
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

    public static final String CONFIGURATION = "configuration";
    public static final String GREMLIN_INPUT_LOCATION = "gremlin.inputLocation";
    public static final String GREMLIN_OUTPUT_LOCATION = "gremlin.outputLocation";
    public static final String GIRAPH_VERTEX_INPUT_FORMAT_CLASS = "giraph.vertexInputFormatClass";
    public static final String GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS = "giraph.vertexOutputFormatClass";

    protected final GiraphGraphVariables variables;

    private GiraphGraph(final Configuration configuration) {
        this.variables = new GiraphGraphVariables(new GiraphConfiguration(configuration));
    }

    public static GiraphGraph open() {
        return GiraphGraph.open(null);
    }

    public static <G extends Graph> G open(final Configuration configuration) {
        final GiraphGraph graph = new GiraphGraph(Optional.ofNullable(configuration).orElse(new BaseConfiguration()));
        return (G) graph;
    }

    public GraphTraversal<Vertex, Vertex> V() {
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<Vertex, Vertex>() {
            public GraphTraversal<Vertex, Vertex> submit(final TraversalEngine engine) {
                if (engine instanceof GraphComputer) {
                    GiraphComputerHelper.prepareTraversalForComputer(this);
                    //this.strategies().register(new ValidateStepsStrategy());
                }
                return super.submit(engine);
            }
        };
        traversal.addStep(new GiraphGraphStep(traversal, Vertex.class, this));
        traversal.memory().set(Key.hidden("g"), this);
        return traversal;
    }

    public GraphTraversal<Edge, Edge> E() {
        final GraphTraversal<Edge, Edge> traversal = new DefaultGraphTraversal<Edge, Edge>() {
            public GraphTraversal<Edge, Edge> submit(final TraversalEngine engine) {
                if (engine instanceof GraphComputer) {
                    GiraphComputerHelper.prepareTraversalForComputer(this);
                    //this.strategies().register(new ValidateStepsStrategy());
                }
                return super.submit(engine);
            }
        };
        traversal.addStep(new GiraphGraphStep(traversal, Edge.class, this));
        traversal.memory().set(Key.hidden("g"), this);
        return traversal;
    }

    public Vertex v(final Object id) {
        return this.V().<Vertex>has(Element.ID, id).next();
    }

    public Edge e(final Object id) {
        return this.E().<Edge>has(Element.ID, id).next();
    }

    public Vertex addVertex(final Object... keyValues) {
        throw Exceptions.vertexAdditionsNotSupported();
    }

    public <C extends GraphComputer> C compute(final Class<C>... graphComputerClass) {
        return (C) new GiraphGraphComputer(this, this.variables().getConfiguration());
    }


    public GiraphGraphVariables variables() {
        return this.variables;
    }

    public String toString() {
        final org.apache.hadoop.conf.Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(this.variables().getConfiguration());
        final String fromString = this.variables().getConfiguration().containsKey(GIRAPH_VERTEX_INPUT_FORMAT_CLASS) ?
                hadoopConfiguration.getClass(GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class).getSimpleName() :
                "none";
        final String toString = this.variables().getConfiguration().containsKey(GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS) ?
                hadoopConfiguration.getClass(GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS, VertexOutputFormat.class).getSimpleName() :
                "none";
        return StringFactory.graphString(this, fromString.toLowerCase() + "->" + toString.toLowerCase());
    }

    public void close() {
        this.variables().getConfiguration().clear();
    }

    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    public GiraphGraph getOutputGraph() {
        final Configuration conf = new BaseConfiguration();
        this.variables().getConfiguration().getKeys().forEachRemaining(key -> {
            try {
                conf.setProperty(key, this.variables().getConfiguration().getString(key));
            } catch (Exception e) {
                // do nothing for serialization problems
            }
        });
        if (this.variables().getConfiguration().containsKey(GREMLIN_OUTPUT_LOCATION)) {
            conf.setProperty(GREMLIN_INPUT_LOCATION, this.variables().getConfiguration().getString(GREMLIN_OUTPUT_LOCATION));
        }
        if (this.variables().getConfiguration().containsKey(GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS)) {
            // TODO: Is this sufficient?
            conf.setProperty(GIRAPH_VERTEX_INPUT_FORMAT_CLASS, this.variables().getConfiguration().getString(GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS).replace("OutputFormat", "InputFormat"));
        }
        return GiraphGraph.open(conf);
    }

    public Features getFeatures() {
        return new Features() {
            @Override
            public GraphFeatures graph() {
                return new GraphFeatures() {
                    @Override
                    public boolean supportsTransactions() {
                        return false;
                    }

                    @Override
                    public boolean supportsThreadedTransactions() {
                        return false;
                    }
                };
            }

            @Override
            public VertexFeatures vertex() {
                return new VertexFeatures() {
                    @Override
                    public boolean supportsAddVertices() {
                        return false;
                    }
                };
            }

            @Override
            public EdgeFeatures edge() {
                return new EdgeFeatures() {
                    @Override
                    public boolean supportsAddEdges() {
                        return false;
                    }
                };
            }
        };
    }
}
