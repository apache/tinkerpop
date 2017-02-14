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
package org.apache.tinkerpop.gremlin.jsr223;

import org.apache.tinkerpop.gremlin.TestHelper;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineSuite.ENGINE_TO_TEST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BindingsScriptEngineTest {

    @Test
    public void shouldIncludeGlobalBindings() throws ScriptException {
        final GremlinScriptEngineManager manager = new DefaultGremlinScriptEngineManager();
        final Bindings b = new SimpleBindings();
        b.put("x", 1);
        b.put("y", 2);

        manager.addPlugin(BindingsGremlinPlugin.build().
                bindings(b).create());

        final GremlinScriptEngine engine = manager.getEngineByName(ENGINE_TO_TEST);
        assertEquals(1, engine.eval("x"));
        assertEquals(2, engine.eval("y"));
    }

    @Test
    public void shouldRemoveGlobalBindings() throws Exception {
        final DefaultGremlinScriptEngineManager manager = new DefaultGremlinScriptEngineManager();
        manager.put("x", 100);

        final GremlinScriptEngine engine1 = manager.getEngineByName(ENGINE_TO_TEST);
        assertThat(engine1.getBindings(ScriptContext.GLOBAL_SCOPE).size(), is(1));
        assertEquals(101, (int) engine1.eval("x+1"));

        manager.getBindings().remove("x");
        final GremlinScriptEngine engine2 = manager.getEngineByName(ENGINE_TO_TEST);
        assertThat(engine2.getBindings(ScriptContext.GLOBAL_SCOPE).size(), is(0));
    }

    @Test
    public void shouldAddLazyGlobalBindingsViaPlugin() throws Exception {
        final Bindings bindings = new SimpleBindings();
        bindings.put("x", 100);
        final BindingsGremlinPlugin plugin = new BindingsGremlinPlugin(() -> bindings);

        final DefaultGremlinScriptEngineManager manager = new DefaultGremlinScriptEngineManager();
        manager.addPlugin(plugin);
        final GremlinScriptEngine engine = manager.getEngineByName(ENGINE_TO_TEST);
        assertThat(engine.getBindings(ScriptContext.GLOBAL_SCOPE).size(), is(1));
        assertEquals(101, (int) engine.eval("x+1"));
    }

    @Test
    public void shouldExtractGlobalBindingsAfterScriptExecution() throws Exception {
        assumeThat("Only works with gremlin-groovy", ENGINE_TO_TEST, is("gremlin-groovy"));
        final GremlinScriptEngineManager manager = new DefaultGremlinScriptEngineManager();

        final Bindings b1 = new SimpleBindings();
        b1.put("x", 1);
        b1.put("y", 2);
        manager.addPlugin(BindingsGremlinPlugin.build().bindings(b1).create());

        final Bindings b2 = new SimpleBindings();
        b2.put("x", 100);
        b2.put("y", 200);
        manager.addPlugin(BindingsGremlinPlugin.build().bindings(b2).create());

        final File scriptFile = TestHelper.generateTempFileFromResource(BindingsScriptEngineTest.class, "bindings-init.groovy", ".groovy");
        final List<String> files = new ArrayList<>();
        files.add(scriptFile.getAbsolutePath());
        manager.addPlugin(ScriptFileGremlinPlugin.build().files(files).create());

        final GremlinScriptEngine engine = manager.getEngineByName(ENGINE_TO_TEST);
        assertEquals(100, engine.eval("x"));
        assertEquals(200, engine.eval("y"));
        assertEquals(300, engine.eval("z"));
        assertEquals(600, engine.eval("addItUp(z, addItUp(x,y))"));

        assertEquals(300, engine.getBindings(ScriptContext.GLOBAL_SCOPE).get("z"));
        assertThat(engine.getBindings(ScriptContext.ENGINE_SCOPE).containsKey("z"), is(false));
    }
}
