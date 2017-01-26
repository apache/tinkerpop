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

import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.Collections;

import static org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineSuite.ENGINE_TO_TEST;
import static org.junit.Assert.assertEquals;

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
                bindings(b).
                scope(ScriptContext.GLOBAL_SCOPE).
                appliesTo(Collections.singletonList(ENGINE_TO_TEST)).create());

        final GremlinScriptEngine engine = manager.getEngineByName(ENGINE_TO_TEST);
        assertEquals(1, engine.eval("x"));
        assertEquals(2, engine.eval("y"));
    }

    @Test
    public void shouldIncludeEngineBindings() throws ScriptException {
        final GremlinScriptEngineManager manager = new DefaultGremlinScriptEngineManager();
        final Bindings b = new SimpleBindings();
        b.put("x", 1);
        b.put("y", 2);

        manager.addPlugin(BindingsGremlinPlugin.build().
                bindings(b).
                scope(ScriptContext.ENGINE_SCOPE).
                appliesTo(Collections.singletonList(ENGINE_TO_TEST)).create());

        final GremlinScriptEngine engine = manager.getEngineByName(ENGINE_TO_TEST);
        assertEquals(1, engine.eval("x"));
        assertEquals(2, engine.eval("y"));
    }

    @Test
    public void shouldIncludeEngineBindingsToOverrideGlobalBindings() throws ScriptException {
        final GremlinScriptEngineManager manager = new DefaultGremlinScriptEngineManager();

        final Bindings b1 = new SimpleBindings();
        b1.put("x", 1);
        b1.put("y", 2);
        manager.addPlugin(BindingsGremlinPlugin.build().
                bindings(b1).
                scope(ScriptContext.GLOBAL_SCOPE).
                appliesTo(Collections.singletonList(ENGINE_TO_TEST)).create());

        final Bindings b2 = new SimpleBindings();
        b2.put("x", 100);
        b2.put("y", 200);
        manager.addPlugin(BindingsGremlinPlugin.build().
                bindings(b2).
                scope(ScriptContext.ENGINE_SCOPE).
                appliesTo(Collections.singletonList(ENGINE_TO_TEST)).create());

        final GremlinScriptEngine engine = manager.getEngineByName(ENGINE_TO_TEST);
        assertEquals(100, engine.eval("x"));
        assertEquals(200, engine.eval("y"));
    }
}
