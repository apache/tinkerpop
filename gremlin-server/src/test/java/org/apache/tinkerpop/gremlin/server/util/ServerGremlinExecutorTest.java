/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.util;

import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.util.TestSupport;
import org.junit.After;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ServerGremlinExecutorTest {

    private static final String TINKERGRAPH_PROPERTIES =
            new java.io.File(TestSupport.getRootOfBuildDirectory(ServerGremlinExecutorTest.class), "../src/test/scripts/tinkergraph-empty.properties")
                    .getAbsolutePath();

    private ServerGremlinExecutor serverGremlinExecutor;

    @After
    public void tearDown() {
        if (serverGremlinExecutor != null) {
            serverGremlinExecutor.getGremlinExecutorService().shutdownNow();
            serverGremlinExecutor.getGraphManager().getGraphNames().forEach(name -> {
                try {
                    serverGremlinExecutor.getGraphManager().getGraph(name).close();
                } catch (Exception ignored) {
                }
            });
        }
    }

    private Settings baseSettings() {
        final Settings settings = new Settings();
        settings.graphs.put("graph", TINKERGRAPH_PROPERTIES);
        settings.gremlinPool = 1;
        return settings;
    }

    @Test
    public void shouldAutoCreateTraversalSourceForSingleGraph() {
        serverGremlinExecutor = new ServerGremlinExecutor(baseSettings(), null, null);

        assertThat(serverGremlinExecutor.getGraphManager().getTraversalSource("g"), is(notNullValue()));
    }

    @Test
    public void shouldAutoCreateTraversalSourceWithPrefixForNonDefaultGraph() {
        final Settings settings = baseSettings();
        settings.graphs.put("myGraph", TINKERGRAPH_PROPERTIES);
        serverGremlinExecutor = new ServerGremlinExecutor(settings, null, null);

        assertThat(serverGremlinExecutor.getGraphManager().getTraversalSource("g"), is(notNullValue()));
        assertThat(serverGremlinExecutor.getGraphManager().getTraversalSource("g_myGraph"), is(notNullValue()));
    }

    @Test
    public void shouldNotAutoCreateTraversalSourceWhenExplicitEntryExists() {
        final Settings settings = baseSettings();
        final Settings.GraphSettings gs = new Settings.GraphSettings();
        gs.configuration = TINKERGRAPH_PROPERTIES;
        final Settings.TraversalSourceSettings tsSettings = new Settings.TraversalSourceSettings();
        tsSettings.name = "g";
        gs.traversalSources.add(tsSettings);
        settings.graphs.put("graph", gs);
        serverGremlinExecutor = new ServerGremlinExecutor(settings, null, null);

        assertThat(serverGremlinExecutor.getGraphManager().getTraversalSource("g"), is(notNullValue()));
        assertThat(serverGremlinExecutor.getGraphManager().getTraversalSource("g_graph"), is(nullValue()));
    }

    @Test
    public void shouldInstantiateLifecycleHooksFromYaml() {
        final Settings settings = baseSettings();
        final Settings.LifeCycleHookSettings hook1 = new Settings.LifeCycleHookSettings();
        hook1.className = TinkerFactoryDataLoader.class.getName();
        hook1.config = new LinkedHashMap<>();
        hook1.config.put("graph", "graph");
        hook1.config.put("dataset", "modern");
        final Settings.LifeCycleHookSettings hook2 = new Settings.LifeCycleHookSettings();
        hook2.className = TinkerFactoryDataLoader.class.getName();
        hook2.config = new LinkedHashMap<>();
        hook2.config.put("graph", "graph");
        hook2.config.put("dataset", "classic");
        final List<Settings.LifeCycleHookSettings> hooks = new ArrayList<>();
        hooks.add(hook1);
        hooks.add(hook2);
        settings.lifecycleHooks = hooks;
        serverGremlinExecutor = new ServerGremlinExecutor(settings, null, null);

        assertThat(serverGremlinExecutor.getHooks().size(), is(2));
        assertThat(serverGremlinExecutor.getHooks().get(0) instanceof TinkerFactoryDataLoader, is(true));
        assertThat(serverGremlinExecutor.getHooks().get(1) instanceof TinkerFactoryDataLoader, is(true));
    }

    @Test
    public void shouldHaveEmptyHooksWhenNoneConfigured() {
        serverGremlinExecutor = new ServerGremlinExecutor(baseSettings(), null, null);

        assertThat(serverGremlinExecutor.getHooks().isEmpty(), is(true));
    }

    @Test
    public void resolveLanguageShouldReturnExplicitLanguage() throws Exception {
        serverGremlinExecutor = new ServerGremlinExecutor(baseSettings(), null, null);

        final Method resolveLanguage = ServerGremlinExecutor.class.getDeclaredMethod("resolveLanguage", String.class);
        resolveLanguage.setAccessible(true);

        assertThat(resolveLanguage.invoke(serverGremlinExecutor, "gremlin-groovy"), is("gremlin-groovy"));
    }

    @Test
    public void resolveLanguageShouldFallBackToGremlinLangWhenNoExplicitLanguage() throws Exception {
        serverGremlinExecutor = new ServerGremlinExecutor(baseSettings(), null, null);

        final Method resolveLanguage = ServerGremlinExecutor.class.getDeclaredMethod("resolveLanguage", String.class);
        resolveLanguage.setAccessible(true);

        assertThat(resolveLanguage.invoke(serverGremlinExecutor, (String) null), is("gremlin-lang"));
        assertThat(resolveLanguage.invoke(serverGremlinExecutor, ""), is("gremlin-lang"));
    }

    @Test
    public void resolveLanguageShouldFallBackToGremlinLangWhenMultipleEngines() throws Exception {
        final Settings settings = baseSettings();
        settings.scriptEngines.put("gremlin-groovy", new Settings.ScriptEngineSettings());
        serverGremlinExecutor = new ServerGremlinExecutor(settings, null, null);

        final Method resolveLanguage = ServerGremlinExecutor.class.getDeclaredMethod("resolveLanguage", String.class);
        resolveLanguage.setAccessible(true);

        assertThat(resolveLanguage.invoke(serverGremlinExecutor, (String) null), is("gremlin-lang"));
    }

    @Test
    public void resolveLanguageShouldUseSoleConfiguredEngine() throws Exception {
        final Settings settings = baseSettings();
        settings.scriptEngines.clear();
        settings.scriptEngines.put("gremlin-groovy", new Settings.ScriptEngineSettings());
        serverGremlinExecutor = new ServerGremlinExecutor(settings, null, null);

        final Method resolveLanguage = ServerGremlinExecutor.class.getDeclaredMethod("resolveLanguage", String.class);
        resolveLanguage.setAccessible(true);

        assertThat(resolveLanguage.invoke(serverGremlinExecutor, (String) null), is("gremlin-groovy"));
    }
}
