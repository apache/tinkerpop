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
package org.apache.tinkerpop.gremlin.hadoop;

import org.apache.commons.configuration.Configuration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopEdge;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopElement;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraphVariables;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopProperty;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopVertex;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopVertexProperty;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.script.ScriptResourceAccess;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class HadoopGraphProvider extends AbstractGraphProvider {

    public static Map<String, String> PATHS = new HashMap<>();
    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(HadoopEdge.class);
        add(HadoopElement.class);
        add(HadoopGraph.class);
        add(HadoopGraphVariables.class);
        add(HadoopProperty.class);
        add(HadoopVertex.class);
        add(HadoopVertexProperty.class);
    }};

    static {
        try {
            final List<String> kryoResources = Arrays.asList(
                    "tinkerpop-modern.kryo",
                    "grateful-dead.kryo",
                    "tinkerpop-classic.kryo",
                    "tinkerpop-crew.kryo");
            for (final String fileName : kryoResources) {
                PATHS.put(fileName, TestHelper.generateTempFileFromResource(GryoResourceAccess.class, fileName, "").getAbsolutePath());
            }

            final List<String> graphsonResources = Arrays.asList(
                    "tinkerpop-modern.json",
                    "grateful-dead.json",
                    "tinkerpop-classic.json",
                    "tinkerpop-crew.json");
            for (final String fileName : graphsonResources) {
                PATHS.put(fileName, TestHelper.generateTempFileFromResource(GraphSONResourceAccess.class, fileName, "").getAbsolutePath());
            }

            final List<String> scriptResources = Arrays.asList(
                    "tinkerpop-classic.txt",
                    "script-input.groovy",
                    "script-output.groovy");
            for (final String fileName : scriptResources) {
                PATHS.put(fileName, TestHelper.generateTempFileFromResource(ScriptResourceAccess.class, fileName, "").getAbsolutePath());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName, final LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, HadoopGraph.class.getName());
            put(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, GraphSONInputFormat.class.getCanonicalName());
            put(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, GryoOutputFormat.class.getCanonicalName());
            put(Constants.GREMLIN_HADOOP_MEMORY_OUTPUT_FORMAT, SequenceFileOutputFormat.class.getCanonicalName());
            put(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, "hadoop-gremlin/target/test-output");
            put(Constants.GREMLIN_HADOOP_DERIVE_MEMORY, true);
            put(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
            /// giraph configuration
            put(GiraphConstants.MIN_WORKERS, 1);
            put(GiraphConstants.MAX_WORKERS, 1);
            put(GiraphConstants.SPLIT_MASTER_WORKER.getKey(), false);
            put(GiraphConstants.ZOOKEEPER_SERVER_PORT.getKey(), 2181);  // you must have a local zookeeper running on this port
            put(GiraphConstants.NETTY_SERVER_USE_EXECUTION_HANDLER.getKey(), false); // this prevents so many integration tests running out of threads
            put(GiraphConstants.NETTY_CLIENT_USE_EXECUTION_HANDLER.getKey(), false); // this prevents so many integration tests running out of threads
            put(GiraphConstants.NUM_INPUT_THREADS.getKey(), 3);
            put(GiraphConstants.NUM_COMPUTE_THREADS.getKey(), 3);
            //put("giraph.vertexOutputFormatThreadSafe", false);
            //put("giraph.numOutputThreads", 3);

            /// spark configuration
            put("spark.master", "local[4]");
            put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            // put("spark.kryo.registrationRequired",true);
        }};
    }

    @Override
    public void clear(final Graph graph, final Configuration configuration) throws Exception {
        if (graph != null)
            graph.close();
    }

    @Override
    public void loadGraphData(final Graph graph, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {
        if (loadGraphWith != null) this.loadGraphDataViaHadoopConfig(graph, loadGraphWith.value());
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    public void loadGraphDataViaHadoopConfig(final Graph g, final LoadGraphWith.GraphData graphData) {
        final String type = "json";

        if (graphData.equals(LoadGraphWith.GraphData.GRATEFUL)) {
            ((HadoopGraph) g).configuration().setInputLocation(PATHS.get("grateful-dead." + type));
        } else if (graphData.equals(LoadGraphWith.GraphData.MODERN)) {
            ((HadoopGraph) g).configuration().setInputLocation(PATHS.get("tinkerpop-modern." + type));
        } else if (graphData.equals(LoadGraphWith.GraphData.CLASSIC)) {
            ((HadoopGraph) g).configuration().setInputLocation(PATHS.get("tinkerpop-classic." + type));
        } else if (graphData.equals(LoadGraphWith.GraphData.CREW)) {
            ((HadoopGraph) g).configuration().setInputLocation(PATHS.get("tinkerpop-crew." + type));
        } else {
            throw new RuntimeException("Could not load graph with " + graphData);
        }
    }
}
