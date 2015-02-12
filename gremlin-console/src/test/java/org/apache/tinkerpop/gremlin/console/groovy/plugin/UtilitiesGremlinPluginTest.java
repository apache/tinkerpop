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
package org.apache.tinkerpop.gremlin.console.groovy.plugin;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class UtilitiesGremlinPluginTest {
    @Test
    public void shouldPluginToAndDoImports() throws Exception {
        final UtilitiesGremlinPlugin plugin = new UtilitiesGremlinPlugin();
        final SpyPluginAcceptor spy = new SpyPluginAcceptor();
        plugin.pluginTo(spy);

        assertEquals(4, spy.getImports().size());
    }

    @Test
    public void shouldFailWithoutUtilitiesPlugin() throws Exception {
        final Groovysh groovysh = new Groovysh();
        try {
            groovysh.execute("describeGraph(g.class)");
            fail("Utilities were not loaded - this should fail.");
        } catch (Exception ignored) {
        }
    }

    @Test
    public void shouldPluginUtilities() throws Exception {
        final UtilitiesGremlinPlugin plugin = new UtilitiesGremlinPlugin();

        final Groovysh groovysh = new Groovysh();
        groovysh.getInterp().getContext().setProperty("g", TinkerFactory.createClassic());

        final Map<String, Object> env = new HashMap<>();
        env.put("ConsolePluginAcceptor.io", new IO());
        env.put("ConsolePluginAcceptor.shell", groovysh);

        final SpyPluginAcceptor spy = new SpyPluginAcceptor(groovysh::execute, () -> env);
        plugin.pluginTo(spy);

        assertThat(groovysh.execute("describeGraph(org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph)").toString(), containsString("IMPLEMENTATION - org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph"));
    }
}
