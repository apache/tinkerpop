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
package org.apache.tinkerpop.gremlin;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.script.ScriptResourceAccess;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A base {@link GraphProvider} that is typically for use with Hadoop-based graphs as it enables access to the various
 * resource data files that are used in the tests.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractFileGraphProvider extends AbstractGraphProvider {

    protected static final Random RANDOM = TestHelper.RANDOM;

    protected boolean graphSONInput = false;

    public static Map<String, String> PATHS = new HashMap<>();
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
    public void clear(final Graph graph, final Configuration configuration) throws Exception {
        if (graph != null)
            graph.close();
    }

    protected String getInputLocation(final Graph g, final LoadGraphWith.GraphData graphData) {
        final String type = this.graphSONInput ? "-v3d0.json" : "-v3d0.kryo";

        if (graphData.equals(LoadGraphWith.GraphData.GRATEFUL))
            return PATHS.get("grateful-dead" + type);
        else if (graphData.equals(LoadGraphWith.GraphData.MODERN))
            return PATHS.get("tinkerpop-modern" + type);
        else if (graphData.equals(LoadGraphWith.GraphData.CLASSIC))
            return PATHS.get("tinkerpop-classic" + type);
        else if (graphData.equals(LoadGraphWith.GraphData.CREW))
            return PATHS.get("tinkerpop-crew" + type);
        else if (graphData.equals(LoadGraphWith.GraphData.SINK))
            return PATHS.get("tinkerpop-sink" + type);
        else
            throw new RuntimeException("Could not load graph with " + graphData);
    }

}
