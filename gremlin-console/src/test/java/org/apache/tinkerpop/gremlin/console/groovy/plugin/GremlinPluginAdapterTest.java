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

import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.console.plugin.PluggedIn;
import org.apache.tinkerpop.gremlin.jsr223.BindingsCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.apache.tinkerpop.gremlin.jsr223.DefaultBindingsCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.DefaultScriptCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.ImportGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.ScriptCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.io.File;
import java.time.DayOfWeek;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinPluginAdapterTest {

    @Test
    public void shouldAdaptForImportCustomizer() throws Exception {
        final ImportGremlinPlugin plugin = ImportGremlinPlugin.build()
                .classImports(java.awt.Color.class, java.sql.CallableStatement.class)
                .enumImports(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY)
                .methodImports(DayOfWeek.class.getMethod("from", TemporalAccessor.class), DayOfWeek.class.getMethod("values")).create();
        final PluggedIn.GremlinPluginAdapter adapter = new PluggedIn.GremlinPluginAdapter(plugin, null, null);

        assertEquals(plugin.getName(), adapter.getName());

        final SpyPluginAcceptor spy = new SpyPluginAcceptor();
        adapter.pluginTo(spy);

        final Set<String> imports = spy.getImports();
        assertEquals(3, imports.size());
        assertThat(imports, hasItems("import " + java.awt.Color.class.getPackage().getName() + ".*",
                                     "import " + java.sql.CallableStatement.class.getPackage().getName() + ".*",
                                     "import static " + DayOfWeek.class.getCanonicalName() + ".*"));
    }

    @Test
    public void shouldAdaptForScriptCustomizer() throws Exception {
        final File scriptFile1 = TestHelper.generateTempFileFromResource(GremlinPluginAdapterTest.class, "script-customizer-1.groovy", ".groovy");
        final File scriptFile2 = TestHelper.generateTempFileFromResource(GremlinPluginAdapterTest.class, "script-customizer-2.groovy", ".groovy");
        final List<String> files = new ArrayList<>();
        files.add(scriptFile1.getAbsolutePath());
        files.add(scriptFile2.getAbsolutePath());
        final ScriptFileGremlinPlugin plugin = ScriptFileGremlinPlugin.build().files(files).create();
        final PluggedIn.GremlinPluginAdapter adapter = new PluggedIn.GremlinPluginAdapter(plugin, null, null);

        assertEquals(plugin.getName(), adapter.getName());

        final List<String> evals = new ArrayList<>();
        final SpyPluginAcceptor spy = new SpyPluginAcceptor(evals::add);
        adapter.pluginTo(spy);

        assertEquals("x = 1 + 1\n" +
                     "y = 10 * x\n" +
                     "z = 1 + x + y", evals.get(0));
        assertEquals("l = g.V(z).out()\n" +
                     "        .group().by('name')", evals.get(1));
    }

    @Test
    public void shouldAdaptForBindingsCustomizer() throws Exception {
        final Bindings bindings = new SimpleBindings();
        bindings.put("x", 1);
        bindings.put("y", "yes");
        bindings.put("z", true);
        final BindingsCustomizer bindingsCustomizer = new DefaultBindingsCustomizer(bindings);
        final GremlinPlugin plugin = new GremlinPlugin() {
            @Override
            public String getName() {
                return "anon-bindings";
            }

            @Override
            public Optional<Customizer[]> getCustomizers(final String scriptEngineName) {
                return Optional.of(new Customizer[]{bindingsCustomizer});
            }
        };
        final PluggedIn.GremlinPluginAdapter adapter = new PluggedIn.GremlinPluginAdapter(plugin, null, null);

        assertEquals(plugin.getName(), adapter.getName());

        final SpyPluginAcceptor spy = new SpyPluginAcceptor();
        adapter.pluginTo(spy);

        final Map<String,Object> bindingsFromSpy = spy.getBindings();
        assertEquals(1, bindingsFromSpy.get("x"));
        assertEquals("yes", bindingsFromSpy.get("y"));
        assertEquals(true, bindingsFromSpy.get("z"));
    }
}
