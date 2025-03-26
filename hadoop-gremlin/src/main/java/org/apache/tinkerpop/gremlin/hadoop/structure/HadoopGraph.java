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

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.AbstractHadoopGraphComputer;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.traversal.strategy.HadoopIoStrategy;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopEdgeIterator;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopVertexIterator;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.AbortiveMultiIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_LIMITED_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_LIMITED_COMPUTER)
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_both_both_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_repeatXoutX_timesX3X_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_repeatXoutX_timesX8X_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "grateful_V_out_out_profile",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "grateful_V_out_out_profileXmetricsX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_hasLabelXsongX_groupXaX_byXnameX_byXproperties_groupCount_byXlabelXX_out_capXaX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest",
        method = "g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byXcountX_capXaX",
        reason = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldStartAndEndWorkersForVertexProgramAndMapReduce",
        reason = "Spark executes map and combine in a lazy fashion and thus, fails the blocking aspect of this test",
        computers = {"org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest",
        method = "*",
        reason = "The interruption model in the test can't guarantee interruption at the right time with HadoopGraph.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionComputerTest",
        method = "*",
        reason = "This test makes use of a sideEffect to enforce when a thread interruption is triggered and thus isn't applicable to HadoopGraph",
        computers = {"org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ReadTest$Traversals",
        method = "g_io_readXxmlX",
        reason = "Hadoop-Gremlin does not support reads/writes with GraphML.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ReadTest$Traversals",
        method = "g_io_read_withXreader_graphmlX",
        reason = "Hadoop-Gremlin does not support reads/writes with GraphML.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteTest$Traversals",
        method = "g_io_writeXxmlX",
        reason = "Hadoop-Gremlin does not support reads/writes with GraphML.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteTest$Traversals",
        method = "g_io_write_withXwriter_graphmlX",
        reason = "Hadoop-Gremlin does not support reads/writes with GraphML.")
public final class HadoopGraph implements Graph {

    public static final Logger LOGGER = LoggerFactory.getLogger(HadoopGraph.class);

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
    }};

    static {
        TraversalStrategies.GlobalCache.registerStrategies(HadoopGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
                        HadoopIoStrategy.instance()));
    }

    protected final HadoopConfiguration configuration;

    private HadoopGraph(final Configuration configuration) {
        this.configuration = new HadoopConfiguration(configuration);
    }

    public static HadoopGraph open() {
        return HadoopGraph.open(EMPTY_CONFIGURATION);
    }

    public static HadoopGraph open(final Configuration configuration) {
        return new HadoopGraph(Optional.ofNullable(configuration).orElse(EMPTY_CONFIGURATION));
    }

    public static HadoopGraph open(final String configurationFile) throws ConfigurationException {
        if (null == configurationFile) throw Graph.Exceptions.argumentCanNotBeNull("configurationFile");
        final Configurations configs = new Configurations();
        return open(configs.properties(configurationFile));
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        throw Exceptions.vertexAdditionsNotSupported();
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        try {
            if (AbstractHadoopGraphComputer.class.isAssignableFrom(graphComputerClass))
                return graphComputerClass.getConstructor(HadoopGraph.class).newInstance(this);
            else
                throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass);
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    public GraphComputer compute() {
        if (this.configuration.containsKey(Constants.GREMLIN_HADOOP_DEFAULT_GRAPH_COMPUTER)) {
            try {
                return this.compute((Class<? extends GraphComputer>) Class.forName(this.configuration.getString(Constants.GREMLIN_HADOOP_DEFAULT_GRAPH_COMPUTER)));
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        } else
            throw new IllegalArgumentException("There is no default GraphComputer for HadoopGraph. Use HadoopGraph.compute(class) or gremlin.hadoop.defaultGraphComputer to specify the GraphComputer to use.");
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
        final String fromString = this.configuration.containsKey(Constants.GREMLIN_HADOOP_GRAPH_READER) ?
                this.configuration.getGraphReader().getSimpleName() :
                "no-reader";
        final String toString = this.configuration.containsKey(Constants.GREMLIN_HADOOP_GRAPH_WRITER) ?
                this.configuration.getGraphWriter().getSimpleName() :
                "no-writer";
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
                // attach vertices if detached instances are passed in otherwise they won't be reloaded and will
                // basically be useless without properties
                final Stream<Vertex> idsThatWereAttachedVertices = Stream.of(vertexIds).
                        filter(id -> id instanceof Vertex).map(
                                id -> id instanceof Attachable ? ((Attachable<Vertex>) id).attach(Attachable.Method.get(this)) : (Vertex) id);

                final Stream<Vertex> verticesFromHadoopGraph = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                        IteratorUtils.filter(new HadoopVertexIterator(this),
                                vertex -> ElementHelper.idExists(vertex.id(), vertexIds)), Spliterator.ORDERED), false);

                // if the vertexIds are all Vertex objects then abort the iteration of the full HadoopGraph as there
                // is no need to refresh the data in this use case as other graphs might
                final AbortiveMultiIterator<Vertex> iterator = new AbortiveMultiIterator<>();
                iterator.addIterator(idsThatWereAttachedVertices.iterator());
                iterator.addIterator(verticesFromHadoopGraph.iterator(), c -> c != vertexIds.length);
                return iterator;
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
                final Stream<Edge> idsThatWereEdges = Stream.of(edgeIds).
                        filter(id -> id instanceof Edge).map(id -> (Edge) id);
                final Stream<Edge> edgesFromHadoopGraph = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                        IteratorUtils.filter(new HadoopEdgeIterator(this),
                                edge -> ElementHelper.idExists(edge.id(), edgeIds)), Spliterator.ORDERED), false);

                // if the edgeIds are all Edge objects then abort the iteration of the full HadoopGraph as there
                // is no need to refresh the data in this use case as other graphs might
                final AbortiveMultiIterator<Edge> iterator = new AbortiveMultiIterator<>();
                iterator.addIterator(idsThatWereEdges.iterator());
                iterator.addIterator(edgesFromHadoopGraph.iterator(), c -> c != edgeIds.length);
                return iterator;
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
                        public boolean supportsRemoveProperty() {
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

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }
    }

}
