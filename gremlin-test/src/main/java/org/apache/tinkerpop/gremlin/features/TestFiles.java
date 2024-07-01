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
package org.apache.tinkerpop.gremlin.features;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.script.ScriptResourceAccess;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TestFiles {
    protected static final Random RANDOM = TestHelper.RANDOM;

    public static final Map<String, String> PATHS = new HashMap<>();
    static {
        try {
            final List<String> kryoResources = Arrays.asList(
                    "tinkerpop-modern-v3.kryo",
                    "grateful-dead-v3.kryo",
                    "tinkerpop-classic-v3.kryo",
                    "tinkerpop-crew-v3.kryo",
                    "tinkerpop-sink-v3.kryo");
            for (final String fileName : kryoResources) {
                PATHS.put(fileName,
                        Storage.toPath(TestHelper.generateTempFileFromResource(GryoResourceAccess.class, fileName, "")));
            }

            final List<String> graphsonResources = Arrays.asList(
                    "tinkerpop-modern-typed-v2.json",
                    "tinkerpop-modern-v3.json",
                    "grateful-dead-typed-v2.json",
                    "grateful-dead-v3.json",
                    "tinkerpop-classic-typed-v2.json",
                    "tinkerpop-classic-v3.json",
                    "tinkerpop-classic-byteid-v3.json",
                    "tinkerpop-classic-byteid-typed-v2.json",
                    "tinkerpop-crew-typed-v2.json",
                    "tinkerpop-crew-v3.json",
                    "tinkerpop-sink-v3.json");
            for (final String fileName : graphsonResources) {
                PATHS.put(fileName,
                        Storage.toPath(TestHelper.generateTempFileFromResource(GraphSONResourceAccess.class, fileName, "")));
            }

            final List<String> scriptResources = Arrays.asList(
                    "tinkerpop-classic.txt",
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

    public static String getInputLocation(final LoadGraphWith.GraphData graphData, final boolean useGraphSON) {
        final String type = useGraphSON ? "-v3.json" : "-v3.kryo";

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
