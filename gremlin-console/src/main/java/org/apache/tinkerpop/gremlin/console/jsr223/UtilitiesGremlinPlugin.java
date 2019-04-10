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
package org.apache.tinkerpop.gremlin.console.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultScriptCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ScriptCustomizer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class UtilitiesGremlinPlugin extends AbstractGremlinPlugin {

    private static final String NAME = "tinkerpop.utilities";

    private static final ScriptCustomizer scripts;

    static {
        try {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(UtilitiesGremlinPlugin.class.getResourceAsStream("UtilitiesGremlinPluginScript.groovy")));
            final List<String> lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            reader.close();

            scripts = new DefaultScriptCustomizer(Collections.singletonList(lines));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public UtilitiesGremlinPlugin() {
        super(NAME, scripts);
    }
}
