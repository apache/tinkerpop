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
package org.apache.tinkerpop.gremlin.server;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import static org.junit.Assert.assertNotNull;

import org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin;
import org.apache.tinkerpop.gremlin.structure.io.Storage;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ServerTestHelper {
    /**
     *  Tests from maven will pass this value in via failsafe plugin. basically we want to home this in on the
     *  gremlin-server directory to get stuff to reference scripts properly from the file system from any directory.
     *  If an overriden path is determined to be absolute then the path is not re-written.
     */
    public static void rewritePathsInGremlinServerSettings(final Settings overridenSettings) {
        Map<String, Map<String, Object>> plugins;
        Map<String, Object> scriptFileGremlinPlugin;
        File homeDir;

        homeDir = new File( getBaseDir(), "../src/test/scripts" );

        plugins = overridenSettings.scriptEngines.get("gremlin-groovy").plugins;
        scriptFileGremlinPlugin = plugins.get(ScriptFileGremlinPlugin.class.getName());

        if (scriptFileGremlinPlugin != null) {
            scriptFileGremlinPlugin
                .put("files",
                     ((List<String>) scriptFileGremlinPlugin.get("files")).stream()
                          .map(s -> new File(s))
                          .map(f -> f.isAbsolute() ? f
                                                   : new File(homeDir, f.getName()))
                          .map(f -> Storage.toPtah(f))
                          .collect(Collectors.toList()));
        }

        overridenSettings.graphs = overridenSettings.graphs.entrySet().stream()
                .map(kv -> {
                    kv.setValue(Storage.toPtah(new File(homeDir, new File(kv.getValue()))));
                    return kv;
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static File getBaseDir() {
      String result;

      result = System.getProperty("build.dir");
      assertNotNull("Expected base.dir system property is set to the project's target directory", result);

      return new File(result);
    }
}
