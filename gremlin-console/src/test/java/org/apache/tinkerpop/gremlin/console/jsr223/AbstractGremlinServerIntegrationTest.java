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

import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.junit.After;
import org.junit.Before;

import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Collections;

/**
 * Starts and stops an instance for each executed test.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinServerIntegrationTest {
    protected GremlinServer server;

    public Settings overrideSettings(final Settings settings) {
        return settings;
    }

    public InputStream getSettingsInputStream() {
        return AbstractGremlinServerIntegrationTest.class.getResourceAsStream("gremlin-server-integration.yaml");
    }

    @Before
    public void setUp() throws Exception {
        final InputStream stream = getSettingsInputStream();
        final Settings settings = Settings.read(stream);

        final Settings overridenSettings = overrideSettings(settings);
        final String prop = Paths.get(AbstractGremlinServerIntegrationTest.class.getResource("tinkergraph-empty.properties").toURI()).toString();
        overridenSettings.graphs.put("graph", prop);
        final String script = Paths.get(AbstractGremlinServerIntegrationTest.class.getResource("generate.groovy").toURI()).toString();
        overridenSettings.scriptEngines.get("gremlin-groovy").plugins
                .get("org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin")
                .put("files", Collections.singletonList(script));

        this.server = new GremlinServer(overridenSettings);

        server.start().join();
    }

    @After
    public void tearDown() throws Exception {
        stopServer();
    }

    public void stopServer() throws Exception {
        server.stop().join();
    }
}
