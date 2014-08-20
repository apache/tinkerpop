package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.computer.util.GiraphComputerHelper;
import com.tinkerpop.gremlin.giraph.process.graph.step.map.GiraphGraphStep;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
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
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.map.MatchTest$JavaMatchTest",
        method = "g_V_matchXa_hasXname_GarciaX__a_inXwrittenByX_b__a_inXsungByX_bX",
        reason = "Giraph-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.map.MatchTest$JavaMatchTest",
        method = "g_V_matchXa_inXsungByX_b__a_inXsungByX_c__b_outXwrittenByX_d__c_outXwrittenByX_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX",
        reason = "Giraph-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldNotAllowBadMemoryKeys",
        reason = "Giraph does a hard kill on failure and stops threads which stops test cases. Exception handling semantics are correct though.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldRequireRegisteringMemoryKeys",
        reason = "Giraph does a hard kill on failure and stops threads which stops test cases. Exception handling semantics are correct though.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.sideEffect.SubgraphTest$JavaSubgraphTest",
        method = "g_v1_outE_subgraphXknowsX",
        reason = "This test directly adds vertices/edges. Giraph does not support those methods of directly adding graph elements.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.sideEffect.SubgraphTest$JavaSubgraphTest",
        method = "g_V_inE_subgraphXcreatedX_name",
        reason = "This test directly adds vertices/edges. Giraph does not support those methods of directly adding graph elements.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.sideEffect.AddEdgeTest$JavaAddEdgeTest",
        method = "g_v1_asXaX_outXcreatedX_inXcreatedX_addBothEXcocreator_aX",
        reason = "This test directly adds vertices/edges. Giraph does not support those methods of directly adding graph elements.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.sideEffect.AddEdgeTest$JavaAddEdgeTest",
        method = "g_v1_asXaX_outXcreatedX_addOutEXcreatedBy_aX",
        reason = "This test directly adds vertices/edges. Giraph does not support those methods of directly adding graph elements.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.sideEffect.AddEdgeTest$JavaAddEdgeTest",
        method = "g_v1_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X",
        reason = "This test directly adds vertices/edges. Giraph does not support those methods of directly adding graph elements.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.filter.HasTest$JavaHasTest",
        method = "g_V_hasXlabelXperson_animalX",
        reason = "This test directly adds vertices/edges. Giraph does not support those methods of directly adding graph elements.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.filter.HasTest$JavaHasTest",
        method = "g_E_hasXlabelXknows_createdX",
        reason = "This test directly adds vertices/edges. Giraph does not support those methods of directly adding graph elements.")
@Graph.FeatureOverride(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
@Graph.FeatureOverride(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
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
        traversal.sideEffects().setGraph(this);
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
        traversal.sideEffects().setGraph(this);
        return traversal;
    }

    @Override
    public <S> GraphTraversal<S, S> of() {
        final GraphTraversal<S, S> traversal = new DefaultGraphTraversal<>();
        traversal.sideEffects().setGraph(this);
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

    public GraphComputer compute(final Class... graphComputerClass) {
        GraphComputerHelper.validateComputeArguments(graphComputerClass);
        if (graphComputerClass.length == 0 || graphComputerClass[0].equals(GiraphGraphComputer.class))
            return new GiraphGraphComputer(this);
        else
            throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass[0]);
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

    public Features features() {
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

                    @Override
                    public boolean supportsCustomIds() {
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

                    @Override
                    public boolean supportsCustomIds() {
                        return false;
                    }
                };
            }
        };
    }
}
