package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.computer.util.GiraphComputerHelper;
import com.tinkerpop.gremlin.giraph.process.graph.step.map.GiraphGraphStep;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
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
            public GraphTraversal<Vertex, Vertex> submit(final GraphComputer computer) {
                GiraphComputerHelper.prepareTraversalForComputer(this);
                return super.submit(computer);
            }
        };
        traversal.addStep(new GiraphGraphStep(traversal, Vertex.class, this));
        traversal.memory().set(Key.hidden("g"), this);
        return traversal;
    }

    public GraphTraversal<Edge, Edge> E() {
        final GraphTraversal<Edge, Edge> traversal = new DefaultGraphTraversal<Edge, Edge>() {
            public GraphTraversal<Edge, Edge> submit(final GraphComputer computer) {
                GiraphComputerHelper.prepareTraversalForComputer(this);
                return super.submit(computer);
            }
        };
        traversal.addStep(new GiraphGraphStep(traversal, Edge.class, this));
        traversal.memory().set(Key.hidden("g"), this);
        return traversal;
    }

    @Override
    public <S> GraphTraversal<S, S> of() {
        final GraphTraversal<S, S> traversal = new DefaultGraphTraversal<>();
        traversal.memory().set(Graph.Key.hidden("g"), this);
        traversal.addStep(new StartStep<>(traversal));
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
        return (C) new GiraphGraphComputer(this);
    }


    public GiraphGraphVariables variables() {
        return this.variables;
    }

    public String toString() {
        final org.apache.hadoop.conf.Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(this.variables().getConfiguration());
        final String fromString = this.variables().getConfiguration().containsKey(Constants.GIRAPH_VERTEX_INPUT_FORMAT_CLASS) ?
                hadoopConfiguration.getClass(Constants.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class).getSimpleName() :
                "none";
        final String toString = this.variables().getConfiguration().containsKey(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS) ?
                hadoopConfiguration.getClass(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS, VertexOutputFormat.class).getSimpleName() :
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
        if (this.variables().getConfiguration().containsKey(Constants.GREMLIN_OUTPUT_LOCATION)) {
            conf.setProperty(Constants.GREMLIN_INPUT_LOCATION, this.variables().getConfiguration().getString(Constants.GREMLIN_OUTPUT_LOCATION) + "/" + Constants.TILDA_G);
        }
        if (this.variables().getConfiguration().containsKey(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS)) {
            // TODO: Is this sufficient?
            conf.setProperty(Constants.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, this.variables().getConfiguration().getString(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS).replace("OutputFormat", "InputFormat"));
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
