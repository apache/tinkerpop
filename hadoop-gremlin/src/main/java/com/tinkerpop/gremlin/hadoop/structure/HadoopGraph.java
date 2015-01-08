package com.tinkerpop.gremlin.hadoop.structure;

import com.tinkerpop.gremlin.hadoop.Constants;
import com.tinkerpop.gremlin.hadoop.process.computer.giraph.GiraphGraphComputer;
import com.tinkerpop.gremlin.hadoop.process.graph.strategy.HadoopElementStepStrategy;
import com.tinkerpop.gremlin.hadoop.structure.hdfs.HadoopEdgeIterator;
import com.tinkerpop.gremlin.hadoop.structure.hdfs.HadoopVertexIterator;
import com.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_COMPUTER)
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.map.MatchTest$StandardTest",
        method = "g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.map.MatchTest$StandardTest",
        method = "g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.map.MatchTest$StandardTest",
        method = "g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.map.GroovyMatchTest$StandardTest",
        method = "g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.map.GroovyMatchTest$StandardTest",
        method = "g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.map.GroovyMatchTest$StandardTest",
        method = "g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.sideEffect.CountTest$StandardTest",
        method = "g_V_both_both_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.sideEffect.CountTest$StandardTest",
        method = "g_V_repeatXoutX_timesX3X_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.sideEffect.CountTest$StandardTest",
        method = "g_V_repeatXoutX_timesX8X_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyCountTest$StandardTest",
        method = "g_V_both_both_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyCountTest$StandardTest",
        method = "g_V_repeatXoutX_timesX3X_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyCountTest$StandardTest",
        method = "g_V_repeatXoutX_timesX8X_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.computer.GroovyGraphComputerTest$ComputerTest",
        method = "shouldNotAllowNullMemoryKeys",
        reason = "Hadoop does a hard kill on failure and stops threads which stops test cases. Exception handling semantics are correct though.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.computer.GroovyGraphComputerTest$ComputerTest",
        method = "shouldNotAllowSettingUndeclaredMemoryKeys",
        reason = "Hadoop does a hard kill on failure and stops threads which stops test cases. Exception handling semantics are correct though.")
@Graph.OptOut(
        test = "com.tinkerpop.gremlin.process.computer.GroovyGraphComputerTest$ComputerTest",
        method = "shouldHaveConsistentMemoryVertexPropertiesAndExceptions",
        reason = "Hadoop does a hard kill on failure and stops threads which stops test cases. Exception handling semantics are correct though.")
public class HadoopGraph implements Graph, Graph.Iterators {

    static {
        try {
            TraversalStrategies.GlobalCache.registerStrategies(HadoopVertex.class, TraversalStrategies.GlobalCache.getStrategies(Vertex.class).clone().addStrategies(HadoopElementStepStrategy.instance()));
            TraversalStrategies.GlobalCache.registerStrategies(HadoopEdge.class, TraversalStrategies.GlobalCache.getStrategies(Edge.class).clone().addStrategies(HadoopElementStepStrategy.instance()));
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public static final Logger LOGGER = LoggerFactory.getLogger(HadoopGraph.class);

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
    }};

    protected final HadoopConfiguration configuration;

    private HadoopGraph(final Configuration configuration) {
        this.configuration = new HadoopConfiguration(configuration);
    }

    public static HadoopGraph open() {
        return HadoopGraph.open(null);
    }

    public static HadoopGraph open(final Configuration configuration) {
        return new HadoopGraph(Optional.ofNullable(configuration).orElse(EMPTY_CONFIGURATION));
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
        throw Exceptions.variablesNotSupported();
    }

    @Override
    public HadoopConfiguration configuration() {
        return this.configuration;
    }

    public String toString() {
        final org.apache.hadoop.conf.Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(this.configuration);
        final String fromString = this.configuration.containsKey(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT) ?
                hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class).getSimpleName() :
                "no-input";
        final String toString = this.configuration.containsKey(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT) ?
                hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, OutputFormat.class).getSimpleName() :
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
    public Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Object... vertexIds) {
        try {
            return 0 == vertexIds.length ? new HadoopVertexIterator(this) : IteratorUtils.filter(new HadoopVertexIterator(this), vertex -> ElementHelper.idExists(vertex.id(), vertexIds));
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public Iterator<Edge> edgeIterator(final Object... edgeIds) {
        try {
            return 0 == edgeIds.length ? new HadoopEdgeIterator(this) : IteratorUtils.filter(new HadoopEdgeIterator(this), edge -> ElementHelper.idExists(edge.id(), edgeIds));
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public Features features() {
        return new HadoopGraphFeatures();
    }

    public static class HadoopGraphFeatures implements Features {

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

                @Override
                public Features.VariableFeatures variables() {
                    return new Features.VariableFeatures() {
                        @Override
                        public boolean supportsVariables() {
                            return false;
                        }

                        @Override
                        public boolean supportsBooleanValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsByteValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsDoubleValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsFloatValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsIntegerValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsLongValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsMapValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsMixedListValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsBooleanArrayValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsByteArrayValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsDoubleArrayValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsFloatArrayValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsIntegerArrayValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsStringArrayValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsLongArrayValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsSerializableValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsStringValues() {
                            return false;
                        }

                        @Override
                        public boolean supportsUniformListValues() {
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
                public Features.VertexPropertyFeatures properties() {
                    return new Features.VertexPropertyFeatures() {
                        @Override
                        public boolean supportsAddProperty() {
                            return false;
                        }
                    };
                }
            };
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }
    }

}
