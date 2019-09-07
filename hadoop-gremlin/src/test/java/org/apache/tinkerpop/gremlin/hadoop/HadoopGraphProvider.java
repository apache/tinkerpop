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
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopEdge;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopElement;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopProperty;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopVertex;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopVertexProperty;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.script.ScriptResourceAccess;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class HadoopGraphProvider extends AbstractGraphProvider {

    protected static final Random RANDOM = new Random();
    private boolean graphSONInput = false;

    public static Map<String, String> PATHS = new HashMap<>();
    public static final Set<Class> IMPLEMENTATION = Collections.unmodifiableSet(new HashSet<Class>() {{
        add(HadoopEdge.class);
        add(HadoopElement.class);
        add(HadoopGraph.class);
        add(HadoopProperty.class);
        add(HadoopVertex.class);
        add(HadoopVertexProperty.class);
        add(ComputerGraph.class);
        add(ComputerGraph.ComputerElement.class);
        add(ComputerGraph.ComputerVertex.class);
        add(ComputerGraph.ComputerEdge.class);
        add(ComputerGraph.ComputerVertexProperty.class);
        add(ComputerGraph.ComputerAdjacentVertex.class);
        add(ComputerGraph.ComputerProperty.class);
    }});

    static {
        try {
            final List<String> kryoResources = Arrays.asList(
                    "tinkerpop-modern-v3d0.kryo",
                    "grateful-dead-v3d0.kryo",
                    "tinkerpop-classic-v3d0.kryo",
                    "tinkerpop-crew-v3d0.kryo",
                    "tinkerpop-sink-v3d0.kryo");
            for (final String fileName : kryoResources) {
                PATHS.put(fileName,
                          Storage.toPath(TestHelper.generateTempFileFromResource(GryoResourceAccess.class, fileName, "")));
            }

            final List<String> graphsonResources = Arrays.asList(
                    "tinkerpop-modern-typed-v2d0.json",
                    "tinkerpop-modern-v3d0.json",
                    "grateful-dead-typed-v2d0.json",
                    "grateful-dead-v3d0.json",
                    "tinkerpop-classic-typed-v2d0.json",
                    "tinkerpop-classic-v3d0.json",
                    "tinkerpop-crew-typed-v2d0.json",
                    "tinkerpop-crew-v3d0.json",
                    "tinkerpop-sink-v3d0.json");
            for (final String fileName : graphsonResources) {
                PATHS.put(fileName,
                          Storage.toPath(TestHelper.generateTempFileFromResource(GraphSONResourceAccess.class, fileName, "")));
            }

            final List<String> scriptResources = Arrays.asList(
                    "tinkerpop-classic.txt",
                    "script-input.groovy",
                    "script-output.groovy",
                    "grateful-dead.txt",
                    "script-input-grateful-dead.groovy",
                    "script-output-grateful-dead.groovy");
            for (final String fileName : scriptResources) {
                PATHS.put(fileName,
                          Storage.toPath(TestHelper.generateTempFileFromResource(ScriptResourceAccess.class, fileName, "")));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName, final LoadGraphWith.GraphData loadGraphWith) {
        this.graphSONInput = RANDOM.nextBoolean();
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, HadoopGraph.class.getName());
            put(Constants.GREMLIN_HADOOP_GRAPH_READER, graphSONInput ? GraphSONInputFormat.class.getCanonicalName() : GryoInputFormat.class.getCanonicalName());
            put(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
            put(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, getWorkingDirectory());
            put(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
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
        final String type = this.graphSONInput ? "-v3d0.json" : "-v3d0.kryo";

        if (graphData.equals(LoadGraphWith.GraphData.GRATEFUL)) {
            ((HadoopGraph) g).configuration().setInputLocation(PATHS.get("grateful-dead" + type));
        } else if (graphData.equals(LoadGraphWith.GraphData.MODERN)) {
            ((HadoopGraph) g).configuration().setInputLocation(PATHS.get("tinkerpop-modern" + type));
        } else if (graphData.equals(LoadGraphWith.GraphData.CLASSIC)) {
            ((HadoopGraph) g).configuration().setInputLocation(PATHS.get("tinkerpop-classic" + type));
        } else if (graphData.equals(LoadGraphWith.GraphData.CREW)) {
            ((HadoopGraph) g).configuration().setInputLocation(PATHS.get("tinkerpop-crew" + type));
        } else if (graphData.equals(LoadGraphWith.GraphData.SINK)) {
            ((HadoopGraph) g).configuration().setInputLocation(PATHS.get("tinkerpop-sink" + type));
        } else {
            throw new RuntimeException("Could not load graph with " + graphData);
        }
    }
}
