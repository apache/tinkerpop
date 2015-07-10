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
import org.apache.tinkerpop.gremlin.groovy.NoImportCustomizerProvider;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

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
    public void shouldEvalSimple() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        assertEquals(3, engine.eval("1+2"));
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
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(NoImportCustomizerProvider.INSTANCE);
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
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(NoImportCustomizerProvider.INSTANCE);
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
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(NoImportCustomizerProvider.INSTANCE);
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
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(NoImportCustomizerProvider.INSTANCE);
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
        final CountDownLatch latch = new CountDownLatch(1);
        final GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine();
        final Thread t = new Thread(() -> {
            try {
                final Object o = scriptEngine.eval("Color.BLACK");
                System.out.println("Should not print: " + o);
                fail.set(true);
            } catch (ScriptException se) {
                // should get here as Color.BLACK is not imported yet.
                System.out.println("Failed to execute Color.BLACK as expected.");
            }

            try {
                int counter = 0;
                while (latch.getCount() == 1) {
                    scriptEngine.eval("1+1");
                    counter++;
                }

                System.out.println(counter + " executions.");

                scriptEngine.eval("Color.BLACK");
                System.out.println("Color.BLACK now evaluates");
            } catch (Exception se) {
                se.printStackTrace();
                fail.set(true);
            }
        }, "test-reload-classloader-1");

        t.start();

        // let the first thead execute a bit.
        Thread.sleep(1000);

        new Thread(() -> {
            System.out.println("Importing java.awt.Color...");
            final Set<String> imports = new HashSet<String>() {{
                add("import java.awt.Color");
            }};
            scriptEngine.addImports(imports);
            latch.countDown();
        }, "test-reload-classloader-2").start();

        t.join();

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
