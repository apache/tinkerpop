/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.hadoop.structure;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.giraph.GiraphGraphComputer;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.hadoop.structure.hdfs.HadoopEdgeIterator;
import org.apache.tinkerpop.gremlin.hadoop.structure.hdfs.HadoopVertexIterator;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_COMPUTER)
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest$Traversals",
        method = "g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest$Traversals",
        method = "g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest$Traversals",
        method = "g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyMatchTest$StandardTraversals",
        method = "g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyMatchTest$StandardTraversals",
        method = "g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyMatchTest$StandardTraversals",
        method = "g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_both_both_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_repeatXoutX_timesX3X_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_repeatXoutX_timesX8X_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyCountTest$StandardTraversals",
        method = "g_V_both_both_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyCountTest$StandardTraversals",
        method = "g_V_repeatXoutX_timesX3X_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyCountTest$StandardTraversals",
        method = "g_V_repeatXoutX_timesX8X_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldNotAllowNullMemoryKeys",
        reason = "Giraph does a hard kill on failure and stops threads which stops test cases. Exception handling semantics are correct though.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldNotAllowSettingUndeclaredMemoryKeys",
        reason = "Giraph does a hard kill on failure and stops threads which stops test cases. Exception handling semantics are correct though.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldHaveConsistentMemoryVertexPropertiesAndExceptions",
        reason = "Giraph does a hard kill on failure and stops threads which stops test cases. Exception handling semantics are correct though.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileTest$Traversals",
        method = "g_V_out_out_grateful_profile",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyProfileTest$StandardTraversals",
        method = "g_V_out_out_grateful_profile",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
public class HadoopGraph implements Graph {

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
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        if (graphComputerClass.equals(GiraphGraphComputer.class))
            return (C) new GiraphGraphComputer(this);
        else if (graphComputerClass.equals(SparkGraphComputer.class))
            return (C) new SparkGraphComputer(this);
        else
            throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass);
    }

    @Override
    public GraphComputer compute() {
        return this.compute(GiraphGraphComputer.class);
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
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        try {
            if (0 == vertexIds.length) {
                return new HadoopVertexIterator(this);
            } else {
                // base the conversion function on the first item in the id list as the expectation is that these
                // id values will be a uniform list
                if (vertexIds[0] instanceof Vertex) {
                    // based on the first item assume all vertices in the argument list
                    if (!Stream.of(vertexIds).allMatch(id -> id instanceof Vertex))
                        throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();

                    // no need to get the vertices again, so just flip it back - some implementation may want to treat this
                    // as a refresh operation. that's not necessary for hadoopgraph.
                    return Stream.of(vertexIds).map(id -> (Vertex) id).iterator();
                } else {
                    final Class<?> firstClass = vertexIds[0].getClass();
                    if (!Stream.of(vertexIds).map(Object::getClass).allMatch(firstClass::equals))
                        throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();     // todo: change exception to be ids of the same type
                    return IteratorUtils.filter(new HadoopVertexIterator(this), vertex -> ElementHelper.idExists(vertex.id(), vertexIds));
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        try {
            if (0 == edgeIds.length) {
                return new HadoopEdgeIterator(this);
            } else {
                // base the conversion function on the first item in the id list as the expectation is that these
                // id values will be a uniform list
                if (edgeIds[0] instanceof Edge) {
                    // based on the first item assume all Edges in the argument list
                    if (!Stream.of(edgeIds).allMatch(id -> id instanceof Edge))
                        throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();

                    // no need to get the vertices again, so just flip it back - some implementation may want to treat this
                    // as a refresh operation. that's not necessary for hadoopgraph.
                    return Stream.of(edgeIds).map(id -> (Edge) id).iterator();
                } else {
                    final Class<?> firstClass = edgeIds[0].getClass();
                    if (!Stream.of(edgeIds).map(Object::getClass).allMatch(firstClass::equals))
                        throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();     // todo: change exception to be ids of the same type
                    return IteratorUtils.filter(new HadoopEdgeIterator(this), vertex -> ElementHelper.idExists(vertex.id(), edgeIds));
                }
            }
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
                public boolean supportsRemoveEdges() {
                    return false;
                }

                @Override
                public boolean supportsAddProperty() {
                    return false;
                }

                @Override
                public boolean supportsRemoveProperty() {
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
                public boolean supportsRemoveVertices() {
                    return false;
                }

                @Override
                public boolean supportsAddProperty() {
                    return false;
                }

                @Override
                public boolean supportsRemoveProperty() {
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

                        @Override
                        public boolean supportsRemoveProperty() {
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
