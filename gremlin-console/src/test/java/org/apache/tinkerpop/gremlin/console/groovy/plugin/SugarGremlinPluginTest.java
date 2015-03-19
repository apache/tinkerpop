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

import org.apache.tinkerpop.gremlin.groovy.plugin.SugarGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.util.MetaRegistryUtil;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * This test lives here (even though the SugarPlugin is over in gremlin-groovy, because it really tests the
 * plugin with respect to the Console.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SugarGremlinPluginTest {
    @Test
    public void shouldPluginSugar() throws Exception {
        MetaRegistryUtil.clearRegistry(new HashSet<>(Arrays.asList(TinkerGraph.class)));

        final SugarGremlinPlugin plugin = new SugarGremlinPlugin();

        final Groovysh groovysh = new Groovysh();

        final Map<String, Object> env = new HashMap<>();
        env.put("ConsolePluginAcceptor.io", new IO());
        env.put("ConsolePluginAcceptor.shell", groovysh);

        final SpyPluginAcceptor spy = new SpyPluginAcceptor(groovysh::execute, () -> env);
        plugin.pluginTo(spy);

        groovysh.getInterp().getContext().setProperty("g", TinkerFactory.createClassic());
        assertEquals(6l, ((GraphTraversal) groovysh.execute("g.traversal().V()")).count().next());
        assertEquals(6l, ((GraphTraversal) groovysh.execute("g.traversal().V")).count().next());
    }
}
