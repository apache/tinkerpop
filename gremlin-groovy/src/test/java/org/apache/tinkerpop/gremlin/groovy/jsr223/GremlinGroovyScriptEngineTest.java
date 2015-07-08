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
import groovy.lang.Script;
import org.apache.tinkerpop.gremlin.groovy.DefaultImportCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.NoImportCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.SecurityCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.ThreadInterruptCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.TimedInterruptCustomizerProvider;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.kohsuke.groovy.sandbox.GroovyInterceptor;
import org.kohsuke.groovy.sandbox.GroovyValueFilter;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
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
    public void shouldSecureAll() throws Exception {
        GroovyInterceptor.getApplicableInterceptors().forEach(GroovyInterceptor::unregister);
        final SecurityCustomizerProvider provider = new SecurityCustomizerProvider(new DenyAll());
        final GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine(
                new DefaultImportCustomizerProvider(), provider);
        try {
            scriptEngine.eval("g = new java.awt.Color(255, 255, 255)");
            fail("Should have failed security");
        } catch (ScriptException se) {
            assertEquals(SecurityException.class, se.getCause().getCause().getClass());
        } finally {
            provider.unregisterInterceptors();
        }
    }

    @Test
    public void shouldSecureSome() throws Exception {
        GroovyInterceptor.getApplicableInterceptors().forEach(GroovyInterceptor::unregister);
        final SecurityCustomizerProvider provider = new SecurityCustomizerProvider(new AllowSome());
        final GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine(
                new DefaultImportCustomizerProvider(), provider);
        try {
            scriptEngine.eval("g = 'new java.awt.Color(255, 255, 255)'");
            fail("Should have failed security");
        } catch (ScriptException se) {
            assertEquals(SecurityException.class, se.getCause().getCause().getClass());
        }

        try {
            final java.awt.Color c = (java.awt.Color) scriptEngine.eval("c = new java.awt.Color(255, 255, 255)");
            assertEquals(java.awt.Color.class, c.getClass());
        } catch (Exception ex) {
            fail("Should not have tossed an exception");
        } finally {
            provider.unregisterInterceptors();
        }
    }

    @Test
    public void shouldProcessScriptWithUTF8Characters() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine();
        assertEquals("轉注", engine.eval("'轉注'"));
    }

    @Test
    public void shouldTimeoutScriptOnTimedWhile() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine(
                new TimedInterruptCustomizerProvider(1000), new DefaultImportCustomizerProvider());
        try {
            engine.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 10000) {}");
            fail("This should have timed out");
        } catch (ScriptException se) {
            assertEquals(TimeoutException.class, se.getCause().getCause().getClass());
        }
    }

    @Test
    public void shouldTimeoutScriptOnTimedWhileOnceEngineHasBeenAliveForLongerThanTimeout() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine(
                new TimedInterruptCustomizerProvider(1000), new DefaultImportCustomizerProvider());
        Thread.sleep(2000);
        try {
            engine.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 10000) {}");
            fail("This should have timed out");
        } catch (ScriptException se) {
            assertEquals(TimeoutException.class, se.getCause().getCause().getClass());
        }

        assertEquals(2, engine.eval("1+1"));
    }

    @Test
    public void shouldContinueToEvalScriptsEvenWithTimedInterrupt() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine(
                new TimedInterruptCustomizerProvider(50), new DefaultImportCustomizerProvider());

        for (int ix = 0; ix < 10; ix++) {
            try {
                // this script takes 10 ms longer than the interruptionTimeout
                engine.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 60) {}");
                fail("This should have timed out");
            } catch (ScriptException se) {
                assertEquals(TimeoutException.class, se.getCause().getCause().getClass());
            }

            // this script takes 10 ms less than the interruptionTimeout
            assertEquals("test", engine.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 40) {};'test'"));
        }

        assertEquals(2, engine.eval("1+1"));
    }

    @Test
    public void shouldInterruptWhile() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine(new ThreadInterruptCustomizerProvider());
        final AtomicBoolean asserted = new AtomicBoolean(false);

        final Thread t = new Thread(() -> {
            try {
                engine.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 10000) {}");
            } catch (ScriptException se) {
                asserted.set(se.getCause().getCause() instanceof InterruptedException);
            }
        });

        t.start();
        t.interrupt();
        while(t.isAlive()) {}

        assertTrue(asserted.get());
    }

    @Test
    public void shouldNotInterruptWhile() throws Exception {
        // companion to shouldInterruptWhile                                 t
        final ScriptEngine engine = new GremlinGroovyScriptEngine();
        final AtomicBoolean exceptionFired = new AtomicBoolean(true);

        final Thread t = new Thread(() -> {
            try {
                engine.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 2000) {};'test'");
                exceptionFired.set(false);
            } catch (ScriptException se) {
                se.printStackTrace();
            }
        });

        t.start();
        t.interrupt();
        while(t.isAlive()) {}

        assertFalse(exceptionFired.get());
    }

    @Test
    public void shouldNotTimeoutStandaloneFunction() throws Exception {
        // use a super fast timeout which should not prevent the call of a cached function
        final ScriptEngine engine = new GremlinGroovyScriptEngine(
                new TimedInterruptCustomizerProvider(1), new DefaultImportCustomizerProvider());
        engine.eval("def addItUp(x,y) { x + y }");

        assertEquals(3, engine.eval("addItUp(1,2)"));
    }

    public static class DenyAll extends GroovyValueFilter {
        @Override
        public Object filter(final Object o) {
            throw new SecurityException("Denied!");
        }
    }

    public static class AllowSome extends GroovyValueFilter {

        public static final Set<Class> ALLOWED_TYPES = new HashSet<Class>() {{
            add(java.awt.Color.class);
            add(Integer.class);
            add(Class.class);
        }};

        @Override
        public Object filter(final Object o) {
            if (null == o || ALLOWED_TYPES.contains(o.getClass()))
                return o;
            if (o instanceof Script || o instanceof Closure)
                return o; // access to properties of compiled groovy script
            throw new SecurityException("Unexpected type: " + o.getClass());
        }
    }
}
