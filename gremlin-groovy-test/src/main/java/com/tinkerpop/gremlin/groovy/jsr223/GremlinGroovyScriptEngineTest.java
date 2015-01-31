package com.tinkerpop.gremlin.groovy.jsr223;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.groovy.DefaultImportCustomizerProvider;
import com.tinkerpop.gremlin.groovy.NoImportCustomizerProvider;
import com.tinkerpop.gremlin.groovy.SecurityCustomizerProvider;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import com.tinkerpop.gremlin.util.config.YamlConfiguration;
import groovy.lang.Closure;
import groovy.lang.Script;
import org.junit.Assert;
import org.junit.Test;
import org.kohsuke.groovy.sandbox.GroovyInterceptor;
import org.kohsuke.groovy.sandbox.GroovyValueFilter;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyScriptEngineTest extends AbstractGremlinTest {

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldDoSomeGremlin() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine();
        final List list = new ArrayList();
        engine.put("g", g);
        engine.put("list", list);
        assertEquals(list.size(), 0);
        engine.eval("g.V(1).out().fill(list)");
        assertEquals(list.size(), 3);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldLoadImports() throws Exception {
        final ScriptEngine engineNoImports = new GremlinGroovyScriptEngine(new NoImportCustomizerProvider());
        try {
            engineNoImports.eval("Vertex.class.getName()");
            fail("Should have thrown an exception because no imports were supplied");
        } catch (Exception se) {
            assertTrue(se instanceof ScriptException);
        }

        final ScriptEngine engineWithImports = new GremlinGroovyScriptEngine(new DefaultImportCustomizerProvider());
        engineWithImports.put("g", g);
        assertEquals(Vertex.class.getName(), engineWithImports.eval("Vertex.class.getName()"));
        assertEquals(2l, engineWithImports.eval("g.V().has('age',Compare.gt,30).count().next()"));
        assertEquals(Direction.IN, engineWithImports.eval("Direction.IN"));
        assertEquals(Direction.OUT, engineWithImports.eval("Direction.OUT"));
        assertEquals(Direction.BOTH, engineWithImports.eval("Direction.BOTH"));
    }


    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldLoadStandardImportsAndThenAddToThem() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(new DefaultImportCustomizerProvider());
        engine.put("g", g);
        assertEquals(Vertex.class.getName(), engine.eval("Vertex.class.getName()"));
        assertEquals(2l, engine.eval("g.V().has('age',Compare.gt,30).count().next()"));
        assertEquals(Direction.IN, engine.eval("Direction.IN"));
        assertEquals(Direction.OUT, engine.eval("Direction.OUT"));
        assertEquals(Direction.BOTH, engine.eval("Direction.BOTH"));

        try {
            engine.eval("YamlConfiguration.class.getName()");
            fail("Should have thrown an exception because no imports were supplied");
        } catch (Exception se) {
            assertTrue(se instanceof ScriptException);
        }

        engine.addImports(new HashSet<>(Arrays.asList("import " + YamlConfiguration.class.getCanonicalName())));
        engine.put("g", g);
        assertEquals(YamlConfiguration.class.getName(), engine.eval("YamlConfiguration.class.getName()"));
        assertEquals(Vertex.class.getName(), engine.eval("Vertex.class.getName()"));
        assertEquals(2l, engine.eval("g.V().has('age',Compare.gt,30).count().next()"));
        assertEquals(Direction.IN, engine.eval("Direction.IN"));
        assertEquals(Direction.OUT, engine.eval("Direction.OUT"));
        assertEquals(Direction.BOTH, engine.eval("Direction.BOTH"));
    }

    @Test
    public void shouldLoadImportsViaDependencyManagerInterface() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(new NoImportCustomizerProvider());
        try {
            engine.eval("Vertex.class.getName()");
            fail("Should have thrown an exception because no imports were supplied");
        } catch (Exception se) {
            assertTrue(se instanceof ScriptException);
        }

        engine.addImports(new HashSet<>(Arrays.asList("import com.tinkerpop.gremlin.structure.Vertex")));
        assertEquals(Vertex.class.getName(), engine.eval("Vertex.class.getName()"));
    }

    @Test
    public void shouldLoadImportsViaDependencyManagerInterfaceAdditively() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(new NoImportCustomizerProvider());
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
            engine.eval("StreamFactory.class.getName()");
            fail("Should have thrown an exception because no imports were supplied");
        } catch (Exception se) {
            assertTrue(se instanceof ScriptException);
        }

        engine.addImports(new HashSet<>(Arrays.asList("import " + StreamFactory.class.getCanonicalName())));
        assertEquals(Vertex.class.getName(), engine.eval("Vertex.class.getName()"));
        assertEquals(StreamFactory.class.getName(), engine.eval("StreamFactory.class.getName()"));
    }

    @Test
    public void shouldLoadImportsViaDependencyManagerFromDependencyGatheredByUse() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(new NoImportCustomizerProvider());
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
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(new NoImportCustomizerProvider());
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
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldProperlyHandleBindings() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine();
        engine.put("g", g);
        Assert.assertEquals(g.V(1).next(), engine.eval("g.V(1).next()"));

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("s", "marko");
        bindings.put("f", 0.5f);
        bindings.put("i", 1);
        bindings.put("b", true);
        bindings.put("l", 100l);
        bindings.put("d", 1.55555d);

        Assert.assertEquals(engine.eval("g.E().has('weight',f).next()", bindings), g.E(7).next());
        Assert.assertEquals(engine.eval("g.V().has('name',s).next()", bindings), g.V(1).next());
        Assert.assertEquals(engine.eval("g.V().sideEffect{it.get().property('bbb',it.get().value('name')=='marko')}.iterate();g.V().has('bbb',b).next()", bindings), g.V(1).next());
        Assert.assertEquals(engine.eval("g.V().sideEffect{it.get().property('iii',it.get().value('name')=='marko'?1:0)}.iterate();g.V().has('iii',i).next()", bindings), g.V(1).next());
        Assert.assertEquals(engine.eval("g.V().sideEffect{it.get().property('lll',it.get().value('name')=='marko'?100l:0l)}.iterate();g.V().has('lll',l).next()", bindings), g.V(1).next());
        Assert.assertEquals(engine.eval("g.V().sideEffect{it.get().property('ddd',it.get().value('name')=='marko'?1.55555d:0)}.iterate();g.V().has('ddd',d).next()", bindings), g.V(1).next());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldBeThreadSafe() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine();

        int runs = 500;
        final CountDownLatch latch = new CountDownLatch(runs);
        final List<String> names = Arrays.asList("marko", "peter", "josh", "vadas", "stephen", "pavel", "matthias");
        final Random random = new Random();

        for (int i = 0; i < runs; i++) {
            new Thread() {
                public void run() {
                    String name = names.get(random.nextInt(names.size() - 1));
                    try {
                        final Bindings bindings = engine.createBindings();
                        bindings.put("g", g);
                        bindings.put("name", name);
                        final Object result = engine.eval("t = g.V().has('name',name); if(t.hasNext()) { t } else { null }", bindings);
                        if (name.equals("stephen") || name.equals("pavel") || name.equals("matthias"))
                            assertNull(result);
                        else
                            assertNotNull(result);
                    } catch (ScriptException e) {
                        assertFalse(true);
                    }
                    latch.countDown();
                }
            }.start();
        }
        latch.await();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldBeThreadSafeOnCompiledScript() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        final CompiledScript script = engine.compile("t = g.V().has('name',name); if(t.hasNext()) { t } else { null }");

        int runs = 500;
        final CountDownLatch latch = new CountDownLatch(runs);
        final List<String> names = Arrays.asList("marko", "peter", "josh", "vadas", "stephen", "pavel", "matthias");
        final Random random = new Random();

        for (int i = 0; i < runs; i++) {
            new Thread() {
                public void run() {
                    String name = names.get(random.nextInt(names.size() - 1));
                    try {
                        final Bindings bindings = engine.createBindings();
                        bindings.put("g", g);
                        bindings.put("name", name);
                        Object result = script.eval(bindings);
                        if (name.equals("stephen") || name.equals("pavel") || name.equals("matthias"))
                            assertNull(result);
                        else
                            assertNotNull(result);
                    } catch (ScriptException e) {
                        //System.out.println(e);
                        assertFalse(true);
                    }
                    latch.countDown();
                }
            }.start();
        }
        latch.await();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldEvalGlobalClosuresEvenAfterEvictionOfClass() throws ScriptException {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);

        // strong referenced global closure
        engine.eval("def isVadas(v){v.value('name')=='vadas'}", bindings);
        assertEquals(true, engine.eval("isVadas(g.V(2).next())", bindings));

        // phantom referenced global closure
        bindings.put(GremlinGroovyScriptEngine.KEY_REFERENCE_TYPE, GremlinGroovyScriptEngine.REFERENCE_TYPE_PHANTOM);
        engine.eval("def isMarko(v){v.value('name')=='marko'}", bindings);

        try {
            engine.eval("isMarko(g.V(1).next())", bindings);
            fail("the isMarko function should not be present");
        } catch (Exception ex) {

        }

        assertEquals(true, engine.eval("def isMarko(v){v.value('name')=='marko'}; isMarko(g.V(1).next())", bindings));

        try {
            engine.eval("isMarko(g.V(1).next())", bindings);
            fail("the isMarko function should not be present");
        } catch (Exception ex) {

        }

        bindings.remove(GremlinGroovyScriptEngine.KEY_REFERENCE_TYPE);

        // isVadas class was a hard reference so it should still be hanging about
        assertEquals(true, engine.eval("isVadas(g.V(2).next())", bindings));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldAllowFunctionsUsedInClosure() throws ScriptException {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("#jsr223.groovy.engine.keep.globals", "phantom");

        // this works on its own when the function and the line that uses it is in one "script".  this is the
        // current workaround
        assertEquals(g.V(2).next(), engine.eval("def isVadas(v){v.value('name')=='vadas'};g.V().filter{isVadas(it.get())}.next()", bindings));

        // let's reset this piece and make sure isVadas is not hanging around.
        engine.reset();

        // validate that isVadas throws an exception since it is not defined
        try {
            engine.eval("isVadas(g.V(2).next())", bindings);

            // fail the test if the above doesn't throw an exception
            fail();
        } catch (Exception ex) {
            // this is good...we want this. it means isVadas isn't hanging about
        }

        // now...define the function separately on its own in one script
        bindings.remove("#jsr223.groovy.engine.keep.globals");
        engine.eval("def isVadas(v){v.value('name')=='vadas'}", bindings);

        // make sure the function works on its own...no problem
        assertEquals(true, engine.eval("isVadas(g.V(2).next())", bindings));

        // make sure the function works in a closure...this generates a StackOverflowError
        assertEquals(g.V(2).next(), engine.eval("g.V().filter{isVadas(it.get())}.next()", bindings));
    }

    @Test
    @org.junit.Ignore
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldAllowUseOfClasses() throws ScriptException {
        GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);

        // works when it's all defined together
        assertEquals(true, engine.eval("class c { static def isVadas(v){v.value('name')=='vadas'}};c.isVadas(g.V(2).next())", bindings));

        // let's reset this piece and make sure isVadas is not hanging around.
        engine.reset();

        // validate that isVadas throws an exception since it is not defined
        try {
            engine.eval("c.isVadas(g.V(2).next())", bindings);

            // fail the test if the above doesn't throw an exception
            fail("Function should be gone");
        } catch (Exception ex) {
            // this is good...we want this. it means isVadas isn't hanging about
        }

        // now...define the class separately on its own in one script...
        // HERE'S an AWKWARD BIT.........
        // YOU HAVE TO END WITH: null;
        // ....OR ELSE YOU GET:
        // javax.script.ScriptException: javax.script.ScriptException:
        // org.codehaus.groovy.runtime.metaclass.MissingMethodExceptionNoStack: No signature of method: c.main()
        // is applicable for argument types: ([Ljava.lang.String;) values: [[]]
        // WOULD BE NICE IF WE DIDN'T HAVE TO DO THAT
        engine.eval("class c { static def isVadas(v){v.name=='vadas'}};null;", bindings);

        // make sure the class works on its own...this generates: groovy.lang.MissingPropertyException: No such property: c for class: Script2
        assertEquals(true, engine.eval("c.isVadas(g.V(2).next())", bindings));
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
        });

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
        }).start();

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
            assertNotNull(g);
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
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldProcessUTF8Query() throws Exception {
        final Vertex nonUtf8 = g.addVertex(T.id, "1", "name", "marko", "age", 29);
        final Vertex utf8Name = g.addVertex(T.id, "2", "name", "轉注", "age", 32);

        final ScriptEngine engine = new GremlinGroovyScriptEngine();

        engine.put("g", g);
        Traversal eval = (Traversal) engine.eval("g.V().has('name', 'marko')");
        assertEquals(nonUtf8, eval.next());
        eval = (Traversal) engine.eval("g.V().has('name','轉注')");
        assertEquals(utf8Name, eval.next());
    }

    public static class DenyAll extends GroovyValueFilter {
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

        public Object filter(final Object o) {
            if (null == o || ALLOWED_TYPES.contains(o.getClass()))
                return o;
            if (o instanceof Script || o instanceof Closure)
                return o; // access to properties of compiled groovy script
            throw new SecurityException("Unexpected type: " + o.getClass());
        }
    }
}
