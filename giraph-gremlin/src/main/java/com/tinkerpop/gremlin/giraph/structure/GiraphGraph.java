package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect.GiraphGraphStep;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.tinkergraph.process.graph.strategy.TinkerElementStepStrategy;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_COMPUTER)
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.map.MatchTest$StandardTest",
        method = "g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX",
        reason = "Giraph-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.map.MatchTest$StandardTest",
        method = "g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX",
        reason = "Giraph-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.map.MatchTest$StandardTest",
        method = "g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX",
        reason = "Giraph-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.map.GroovyMatchTest$StandardTest",
        method = "g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX",
        reason = "Giraph-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.map.GroovyMatchTest$StandardTest",
        method = "g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX",
        reason = "Giraph-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.map.GroovyMatchTest$StandardTest",
        method = "g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX",
        reason = "Giraph-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.sideEffect.CountTest$StandardTest",
        method = "g_V_both_both_count",
        reason = "Giraph-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.sideEffect.CountTest$StandardTest",
        method = "g_V_asXaX_out_jumpXa_loops_lt_3X_count",
        reason = "Giraph-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyCountTest$StandardTest",
        method = "g_V_both_both_count",
        reason = "Giraph-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyCountTest$StandardTest",
        method = "g_V_asXaX_out_jumpXa_loops_lt_3X_count",
        reason = "Giraph-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.computer.GroovyGraphComputerTest$ComputerTest",
        method = "shouldNotAllowNullMemoryKeys",
        reason = "Giraph does a hard kill on failure and stops threads which stops test cases. Exception handling semantics are correct though.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.computer.GroovyGraphComputerTest$ComputerTest",
        method = "shouldNotAllowSettingUndeclaredMemoryKeys",
        reason = "Giraph does a hard kill on failure and stops threads which stops test cases. Exception handling semantics are correct though.")
public class GiraphGraph implements Graph {

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, GiraphGraph.class.getName());
    }};


    protected final GiraphGraphVariables variables;
    protected final GiraphConfiguration configuration;

    private GiraphGraph(final Configuration configuration) {
        this.configuration = new GiraphConfiguration(configuration);
        this.variables = new GiraphGraphVariables();
    }

    public static GiraphGraph open() {
        return GiraphGraph.open(null);
    }

    public static GiraphGraph open(final Configuration configuration) {
        return new GiraphGraph(Optional.ofNullable(configuration).orElse(EMPTY_CONFIGURATION));
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V() {
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(this);
        return traversal.addStep(new GiraphGraphStep<>(traversal, Vertex.class, this));
    }

    @Override
    public GraphTraversal<Edge, Edge> E() {
        final GraphTraversal<Edge, Edge> traversal = new DefaultGraphTraversal<>(this);
        return traversal.addStep(new GiraphGraphStep<>(traversal, Edge.class, this));
    }

    @Override
    public <S> GraphTraversal<S, S> of() {
        final GraphTraversal<S, S> traversal = new DefaultGraphTraversal<>(this);
        return traversal.addStep(new StartStep<>(traversal));
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        throw Exceptions.vertexAdditionsNotSupported();
    }

    @Override
    public GraphComputer compute(final Class... graphComputerClass) {
        GraphComputerHelper.validateComputeArguments(graphComputerClass);
        if (graphComputerClass.length == 0 || graphComputerClass[0].equals(GiraphGraphComputer.class))
            return new GiraphGraphComputer(this);
        else
            throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass[0]);
    }


    @Override
    public Variables variables() {
        return this.variables;
    }

    @Override
    public GiraphConfiguration configuration() {
        return this.configuration;
    }

    public String toString() {
        final org.apache.hadoop.conf.Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(this.configuration);
        final String fromString = this.configuration.containsKey(Constants.GIRAPH_VERTEX_INPUT_FORMAT_CLASS) ?
                hadoopConfiguration.getClass(Constants.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class).getSimpleName() :
                "no-input";
        final String toString = this.configuration.containsKey(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS) ?
                hadoopConfiguration.getClass(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS, VertexOutputFormat.class).getSimpleName() :
                "no-output";
        return StringFactory.graphString(this, fromString.toLowerCase() + "->" + toString.toLowerCase());
    }

    @Override
    public void close() {
        this.configuration.clear();
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
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
                    public boolean supportsAddProperty() {
                        return false;
                    }

                    @Override
                    public boolean supportsCustomIds() {
                        return false;
                    }

                    @Override
                    public VertexPropertyFeatures properties() {
                        return new VertexPropertyFeatures() {
                            @Override
                            public boolean supportsAddProperty() {
                                return false;
                            }
                        };
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
                    public boolean supportsAddProperty() {
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
