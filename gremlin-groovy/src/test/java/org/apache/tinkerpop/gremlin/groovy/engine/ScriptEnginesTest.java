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
package org.apache.tinkerpop.gremlin.groovy.engine;

import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.awt.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ScriptEnginesTest {
    @Test
    public void shouldMergeBindingsFromLocalAndGlobal() throws Exception {
        final ScriptEngines engines = new ScriptEngines(se -> {});
        engines.reload("gremlin-groovy", Collections.<String>emptySet(),
                Collections.<String>emptySet(), Collections.emptyMap());

        engines.loadPlugins(Arrays.asList(new GremlinPlugin() {
            @Override
            public String getName() {
                return "mock";
            }

            @Override
            public void pluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
                pluginAcceptor.addBinding("y", "here");
            }
        }));

        final Bindings localBindings = new SimpleBindings();
        localBindings.put("x", "there");

        assertEquals("herethere", engines.eval("y+x", localBindings, "gremlin-groovy"));
    }

    @Test
    public void shouldMergeBindingsFromLocalAndGlobalWithMultiplePlugins() throws Exception {
        final ScriptEngines engines = new ScriptEngines(se -> {});
        engines.reload("gremlin-groovy", Collections.<String>emptySet(),
                Collections.<String>emptySet(), Collections.emptyMap());

        engines.loadPlugins(Arrays.asList(new GremlinPlugin() {
            @Override
            public String getName() {
                return "mock1";
            }

            @Override
            public void pluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
                pluginAcceptor.addBinding("y", "here");
            }
        }));

        final Bindings localBindings = new SimpleBindings();
        localBindings.put("x", "there");

        assertEquals("herethere", engines.eval("y+x", localBindings, "gremlin-groovy"));

        engines.loadPlugins(Arrays.asList(new GremlinPlugin() {
            @Override
            public String getName() {
                return "mock2";
            }

            @Override
            public void pluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
                pluginAcceptor.addBinding("z", "where");
                pluginAcceptor.addImports(new HashSet<>(Arrays.asList("import java.awt.Color")));
            }
        }));

        assertEquals("heretherewhere", engines.eval("y+x+z", localBindings, "gremlin-groovy"));
        assertEquals(Color.RED, engines.eval("Color.RED", localBindings, "gremlin-groovy"));

    }

    @Test
    public void shouldMergeBindingsWhereLocalOverridesGlobal() throws Exception {
        final ScriptEngines engines = new ScriptEngines(se -> {});
        engines.reload("gremlin-groovy", Collections.<String>emptySet(),
                Collections.<String>emptySet(), Collections.emptyMap());

        engines.loadPlugins(Arrays.asList(new GremlinPlugin() {
            @Override
            public String getName() {
                return "mock";
            }

            @Override
            public void pluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
                pluginAcceptor.addBinding("y", "here");
            }
        }));

        // the "y" below should override the global variable setting.
        final Bindings localBindings = new SimpleBindings();
        localBindings.put("y", "there");
        localBindings.put("z", "where");

        assertEquals("therewhere", engines.eval("y+z", localBindings, "gremlin-groovy"));
    }

    @Test
    public void shouldFailUntilImportExecutes() throws Exception {
        final ScriptEngines engines = new ScriptEngines(se -> {});
        engines.reload("gremlin-groovy", Collections.<String>emptySet(),
                Collections.<String>emptySet(), Collections.emptyMap());

        final Set<String> imports = new HashSet<String>() {{
            add("import java.awt.Color");
        }};

        final AtomicInteger successes = new AtomicInteger(0);
        final AtomicInteger failures = new AtomicInteger(0);

        final Thread threadImport = new Thread(() -> {
            engines.addImports(imports);
        });

        // issue 1000 scripts in one thread using a class that isn't imported.  this will result in failure.
        // while that thread is running start a new thread that issues an addImports to include that class.
        // this should block further evals in the first thread until the import is complete at which point
        // evals in the first thread will resume and start to succeed
        final Thread threadEvalAndTriggerImport = new Thread(() ->
            IntStream.range(0, 1000).forEach(i -> {
                try {
                    engines.eval("Color.BLACK", new SimpleBindings(), "gremlin-groovy");
                    successes.incrementAndGet();
                } catch (Exception ex) {
                    if (failures.incrementAndGet() == 500) threadImport.start();
                }
            })
        );

        threadEvalAndTriggerImport.start();

        threadEvalAndTriggerImport.join();
        threadImport.join();

        assertTrue(successes.intValue() > 0);
        assertTrue(failures.intValue() >= 500);

        engines.close();
    }
}
