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

import org.apache.tinkerpop.gremlin.console.plugin.PluggedIn;
import org.apache.tinkerpop.gremlin.jsr223.BindingsCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.ScriptCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin;
import org.junit.Test;

import java.time.DayOfWeek;
import java.time.temporal.TemporalAccessor;
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
        final PluggedIn.GremlinPluginAdapter adapter = new PluggedIn.GremlinPluginAdapter(plugin);

        assertEquals(plugin.getName(), adapter.getName());

        final SpyPluginAcceptor spy = new SpyPluginAcceptor();
        adapter.pluginTo(spy);

        final Set<String> imports = spy.getImports();
        assertEquals(6, imports.size());
        assertThat(imports, hasItems("import " + java.awt.Color.class.getCanonicalName()));
        assertThat(imports, hasItems("import " + java.sql.CallableStatement.class.getCanonicalName()));
        assertThat(imports, hasItems("import static " + DayOfWeek.class.getCanonicalName() + "." + DayOfWeek.SATURDAY.name()));
        assertThat(imports, hasItems("import static " + DayOfWeek.class.getCanonicalName() + "." + DayOfWeek.SUNDAY.name()));
        assertThat(imports, hasItems("import static " + DayOfWeek.class.getCanonicalName() + ".from"));
        assertThat(imports, hasItems("import static " + DayOfWeek.class.getCanonicalName() + ".values"));
    }
}
