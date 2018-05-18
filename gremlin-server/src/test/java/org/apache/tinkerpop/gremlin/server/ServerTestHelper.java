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
import java.util.Map;
import java.util.stream.Collectors;

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
        final String buildDir = System.getProperty("build.dir");
        final String homeDir = buildDir.substring(0, buildDir.indexOf("gremlin-server") + "gremlin-server".length()) +
                File.separator + "src" + File.separator + "test" + File.separator +"scripts";

        overridenSettings.scriptEngines.get("gremlin-groovy").scripts = overridenSettings.scriptEngines
                .get("gremlin-groovy").scripts.stream()
                .map(s -> new File(s).isAbsolute() ? s : homeDir + s.substring(s.lastIndexOf(File.separator)))
                .collect(Collectors.toList());

        overridenSettings.graphs = overridenSettings.graphs.entrySet().stream()
                .map(kv -> {
                    kv.setValue(homeDir + kv.getValue().substring(kv.getValue().lastIndexOf(File.separator)));
                    return kv;
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
