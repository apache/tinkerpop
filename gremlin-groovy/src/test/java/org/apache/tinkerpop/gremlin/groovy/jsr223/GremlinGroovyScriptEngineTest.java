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
package org.apache.tinkerpop.gremlin.groovy.jsr223;

import groovy.lang.Closure;
import org.apache.tinkerpop.gremlin.groovy.CompilerCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.NoImportCustomizerProvider;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.awt.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyScriptEngineTest {
    private static final Logger logger = LoggerFactory.getLogger(GremlinGroovyScriptEngineTest.class);

    @Test
    public void shouldCompileScriptWithoutRequiringVariableBindings() throws Exception {
        // compile() should cache the script to avoid future compilation
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

        final String script = "g.V(x).out()";
        assertFalse(engine.isCached(script));
        assertNotNull(engine.compile(script));
        assertTrue(engine.isCached(script));

        engine.reset();

        assertFalse(engine.isCached(script));
    }

    @Test
    public void shouldEvalWithNoBindings() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        assertEquals(3, engine.eval("1+2"));
    }

    @Test
    public void shouldEvalWithBindings() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        final Bindings b = new SimpleBindings();
        b.put("x", 2);
        assertEquals(3, engine.eval("1+x", b));
    }

    @Test
    public void shouldEvalWithNullInBindings() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        final Bindings b = new SimpleBindings();
        b.put("x", null);
        assertNull(engine.eval("x", b));
    }

    @Test
    public void shouldEvalSuccessfulAssert() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        assertNull(engine.eval("assert 1==1"));
    }

    @Test(expected = AssertionError.class)
    public void shouldEvalFailingAssert() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        engine.eval("assert 1==0");
    }

    @Test
    public void shouldLoadImportsViaDependencyManagerInterface() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine((CompilerCustomizerProvider) NoImportCustomizerProvider.INSTANCE);
        try {
            engine.eval("Vertex.class.getName()");
            fail("Should have thrown an exception because no imports were supplied");
        } catch (Exception se) {
            assertTrue(se instanceof ScriptException);
        }

        engine.addImports(new HashSet<>(Arrays.asList("import org.apache.tinkerpop.gremlin.structure.Vertex")));
        assertEquals(Vertex.class.getName(), engine.eval("Vertex.class.getName()"));
    }

    @Test
    public void shouldLoadImportsViaDependencyManagerInterfaceAdditively() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine((CompilerCustomizerProvider) NoImportCustomizerProvider.INSTANCE);
        try {
            engine.eval("Vertex.class.getName()");
            fail("Should have thrown an exception because no imports were supplied");
        } catch (Exception se) {
            assertTrue(se instanceof ScriptException);
        }

        try {
            engine.eval("StreamFactory.class.getName()");
            fail("Should have thrown an exception because no imports were supplied");
        } catch (Exception se) {
            assertTrue(se instanceof ScriptException);
        }

        engine.addImports(new HashSet<>(Arrays.asList("import " + Vertex.class.getCanonicalName())));
        assertEquals(Vertex.class.getName(), engine.eval("Vertex.class.getName()"));

        try {
            engine.eval("IteratorUtils.class.getName()");
            fail("Should have thrown an exception because no imports were supplied");
        } catch (Exception se) {
            assertTrue(se instanceof ScriptException);
        }

        engine.addImports(new HashSet<>(Arrays.asList("import " + IteratorUtils.class.getCanonicalName())));
        assertEquals(Vertex.class.getName(), engine.eval("Vertex.class.getName()"));
        assertEquals(IteratorUtils.class.getName(), engine.eval("IteratorUtils.class.getName()"));
    }

    @Test
    public void shouldLoadImportsViaDependencyManagerFromDependencyGatheredByUse() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine((CompilerCustomizerProvider) NoImportCustomizerProvider.INSTANCE);
        try {
            engine.eval("org.apache.commons.math3.util.FastMath.abs(-1235)");
            fail("Should have thrown an exception because no imports were supplied");
        } catch (Exception se) {
            assertTrue(se instanceof ScriptException);
        }

        engine.addImports(new HashSet<>(Arrays.asList("import org.apache.commons.math3.util.FastMath")));
        engine.use("org.apache.commons", "commons-math3", "3.2");
        assertEquals(1235, engine.eval("org.apache.commons.math3.util.FastMath.abs(-1235)"));
    }

    @Test
    public void shouldAllowsUseToBeExecutedAfterImport() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine((CompilerCustomizerProvider) NoImportCustomizerProvider.INSTANCE);
        try {
            engine.eval("org.apache.commons.math3.util.FastMath.abs(-1235)");
            fail("Should have thrown an exception because no imports were supplied");
        } catch (Exception se) {
            assertTrue(se instanceof ScriptException);
        }

        engine.use("org.apache.commons", "commons-math3", "3.2");
        engine.addImports(new HashSet<>(Arrays.asList("import org.apache.commons.math3.util.FastMath")));
        assertEquals(1235, engine.eval("org.apache.commons.math3.util.FastMath.abs(-1235)"));
    }

    @Test
    public void shouldAllowsMultipleImports() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine((CompilerCustomizerProvider) NoImportCustomizerProvider.INSTANCE);
        try {
            engine.eval("Color.RED");
            fail("Should have thrown an exception because no imports were supplied");
        } catch (Exception se) {
            assertTrue(se instanceof ScriptException);
        }

        try {
            engine.eval("SystemColor.ACTIVE_CAPTION");
            fail("Should have thrown an exception because no imports were supplied");
        } catch (Exception se) {
            assertTrue(se instanceof ScriptException);
        }

        engine.addImports(new HashSet<>(Arrays.asList("import java.awt.Color")));
        assertEquals(Color.RED, engine.eval("Color.RED"));

        engine.addImports(new HashSet<>(Arrays.asList("import java.awt.SystemColor")));
        assertEquals(Color.RED, engine.eval("Color.RED"));
        assertEquals(SystemColor.ACTIVE_CAPTION, engine.eval("SystemColor.ACTIVE_CAPTION"));
    }

    @Test
    public void shouldClearEngineScopeOnReset() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        engine.eval("x = { y -> y + 1}");
        Bindings b = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
        assertTrue(b.containsKey("x"));
        assertEquals(2, ((Closure) b.get("x")).call(1));

        // should clear the bindings
        engine.reset();
        try {
            engine.eval("x(1)");
            fail("Bindings should have been cleared.");
        } catch (Exception ex) {

        }

        b = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
        assertFalse(b.containsKey("x"));

        // redefine x
        engine.eval("x = { y -> y + 2}");
        assertEquals(3, engine.eval("x(1)"));
        b = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
        assertTrue(b.containsKey("x"));
        assertEquals(3, ((Closure) b.get("x")).call(1));
    }

    @Test
    public void shouldReloadClassLoaderWhileDoingEvalInSeparateThread() throws Exception {
        final AtomicBoolean fail = new AtomicBoolean(false);
        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Color> color = new AtomicReference<>(Color.RED);

        final GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine();

        try {
            scriptEngine.eval("Color.BLACK");
            fail("Should fail as class is not yet imported");
        } catch (ScriptException se) {
            // should get here as Color.BLACK is not imported yet.
            logger.info("Failed to execute Color.BLACK as expected.");
        }

        final Thread evalThread = new Thread(() -> {
            try {
                // execute scripts until the other thread releases this latch (i.e. after import)
                while (latch.getCount() == 1) {
                    scriptEngine.eval("1+1");
                    counter.incrementAndGet();
                }

                color.set((Color) scriptEngine.eval("Color.BLACK"));
            } catch (Exception se) {
                fail.set(true);
            }
        }, "test-reload-classloader-1");

        evalThread.start();

        // let the first thread execute a bit.
        Thread.sleep(1000);

        final Thread importThread = new Thread(() -> {
            logger.info("Importing java.awt.Color...");
            final Set<String> imports = new HashSet<String>() {{
                add("import java.awt.Color");
            }};
            scriptEngine.addImports(imports);
            latch.countDown();
        }, "test-reload-classloader-2");

        importThread.start();

        // block until both threads are done
        importThread.join();
        evalThread.join();

        assertEquals(Color.BLACK, color.get());
        assertThat(counter.get(), greaterThan(0));
        assertFalse(fail.get());
    }

    @Test
    public void shouldResetClassLoader() throws Exception {
        final GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine();
        try {
            scriptEngine.eval("addOne(1)");
            fail("Should have tossed ScriptException since addOne is not yet defined.");
        } catch (ScriptException se) {
        }

        // validate that the addOne function works
        scriptEngine.eval("addOne = { y-> y + 1}");
        assertEquals(2, scriptEngine.eval("addOne(1)"));

        // reset the script engine which should blow out the addOne function that's there.
        scriptEngine.reset();

        try {
            scriptEngine.eval("addOne(1)");
            fail("Should have tossed ScriptException since addOne is no longer defined after reset.");
        } catch (ScriptException se) {
        }
    }

    @Test
    public void shouldProcessScriptWithUTF8Characters() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine();
        assertEquals("轉注", engine.eval("'轉注'"));
    }
}