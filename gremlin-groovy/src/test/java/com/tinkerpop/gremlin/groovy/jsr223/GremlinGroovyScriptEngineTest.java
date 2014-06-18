package com.tinkerpop.gremlin.groovy.jsr223;

import com.tinkerpop.gremlin.groovy.DefaultImportCustomizerProvider;
import com.tinkerpop.gremlin.groovy.NoImportCustomizerProvider;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import groovy.lang.Closure;
import org.junit.Test;

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
	public void shouldDoSomeGremlin() throws Exception {
		final ScriptEngine engine = new GremlinGroovyScriptEngine();
		final List list = new ArrayList();
		engine.put("g", TinkerFactory.createClassic());
		engine.put("list", list);
		assertEquals(list.size(), 0);
		engine.eval("g.v(1).out.fill(list)");
		assertEquals(list.size(), 3);
	}

	@Test
	public void shouldLoadImports() throws Exception {
		final ScriptEngine engineNoImports = new GremlinGroovyScriptEngine(100, new NoImportCustomizerProvider());
		try {
			engineNoImports.eval("Vertex.class.getName()");
			fail("Should have thrown an exception because no imports were supplied");
		} catch (Exception se) {
			assertTrue(se instanceof ScriptException);
		}

		final ScriptEngine engineWithImports = new GremlinGroovyScriptEngine(100, new DefaultImportCustomizerProvider());
		assertEquals(Vertex.class.getName(), engineWithImports.eval("Vertex.class.getName()"));
		assertEquals(2l, engineWithImports.eval("TinkerFactory.createClassic().V.has('age',T.gt,30).count()"));
		assertEquals(Direction.IN, engineWithImports.eval("Direction.IN"));
		assertEquals(Direction.OUT, engineWithImports.eval("Direction.OUT"));
		assertEquals(Direction.BOTH, engineWithImports.eval("Direction.BOTH"));
	}

	@Test
	public void shouldProperlyHandleBindings() throws Exception {
		final TinkerGraph g = TinkerFactory.createClassic();
		final ScriptEngine engine = new GremlinGroovyScriptEngine();
		assertTrue(engine.eval("g = TinkerFactory.createClassic()") instanceof TinkerGraph);
		assertTrue(engine.get("g") instanceof TinkerGraph);
		assertEquals(g.v(1), engine.eval("g.v(1)"));

		final Bindings bindings = engine.createBindings();
		bindings.put("g", g);
		bindings.put("s", "marko");
		bindings.put("f", 0.5f);
		bindings.put("i", 1);
		bindings.put("b", true);
		bindings.put("l", 100l);
		bindings.put("d", 1.55555d);

		assertEquals(engine.eval("g.E.has('weight',f).next()", bindings), g.e(7));
		assertEquals(engine.eval("g.V.has('name',s).next()", bindings), g.v(1));
		assertEquals(engine.eval("g.V.sideEffect{it.property('bbb',it.name=='marko')}.iterate();g.V.has('bbb',b).next()", bindings), g.v(1));
		assertEquals(engine.eval("g.V.sideEffect{it.property('iii',it.name=='marko'?1:0)}.iterate();g.V.has('iii',i).next()", bindings), g.v(1));
		assertEquals(engine.eval("g.V.sideEffect{it.property('lll',it.name=='marko'?100l:0l)}.iterate();g.V.has('lll',l).next()", bindings), g.v(1));
		assertEquals(engine.eval("g.V.sideEffect{it.property('ddd',it.name=='marko'?1.55555d:0)}.iterate();g.V.has('ddd',d).next()", bindings), g.v(1));
	}

	@Test
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
						bindings.put("g", TinkerFactory.createClassic());
						bindings.put("name", name);
						final Object result = engine.eval("t = g.V.has('name',name); if(t.hasNext()) { t.out.count() } else { null }", bindings);
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
	public void shouldBeThreadSafeOnCompiledScript() throws Exception {
		final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
		final CompiledScript script = engine.compile("t = g.V.has('name',name); if(t.hasNext()) { t.out.count() } else { null }");

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
						bindings.put("g", TinkerFactory.createClassic());
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
	@org.junit.Ignore
	public void shouldNotBlowTheHeapParameterized() throws ScriptException {
		final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
		final Graph g = TinkerFactory.createClassic();

		final String[] gremlins = new String[]{
				"g.v(xxx).out.toList()",
				"g.v(xxx).in.toList()",
				"g.v(xxx).out.out.out.toList()",
				"g.v(xxx).out.groupCount()"
		};

		long parameterizedStartTime = System.currentTimeMillis();
		System.out.println("Try to blow the heap with parameterized Gremlin.");
		try {
			for (int ix = 0; ix < 50001; ix++) {
				final Bindings bindings = engine.createBindings();
				bindings.put("g", g);
				bindings.put("xxx", ((ix % 4) + 1));
				engine.eval(gremlins[ix % 4], bindings);

				if (ix > 0 && ix % 5000 == 0) {
					System.out.println(String.format("%s scripts processed in %s (ms) - rate %s (ms/q).", ix, System.currentTimeMillis() - parameterizedStartTime, Double.valueOf(System.currentTimeMillis() - parameterizedStartTime) / Double.valueOf(ix)));
				}
			}
		} catch (OutOfMemoryError oome) {
			fail("Blew the heap - the cache should prevent this from happening.");
		} finally {
			System.out.println(engine.stats());
		}
	}

	@Test
	@org.junit.Ignore("Run this test as needed - it slows the build terribly")
	public void shouldNotBlowTheHeapUnparameterized() throws ScriptException {
		final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
		long notParameterizedStartTime = System.currentTimeMillis();
		System.out.println("Try to blow the heap with non-parameterized Gremlin.");
		try {
			for (int ix = 0; ix < 15001; ix++) {
				final Bindings bindings = engine.createBindings();
				engine.eval(String.format("1+%s", ix), bindings);
				if (ix > 0 && ix % 5000 == 0) {
					System.out.println(String.format("%s scripts processed in %s (ms) - rate %s (ms/q).", ix, System.currentTimeMillis() - notParameterizedStartTime, Double.valueOf(System.currentTimeMillis() - notParameterizedStartTime) / Double.valueOf(ix)));
				}
			}
		} catch (OutOfMemoryError oome) {
			fail("Blew the heap - the cache should prevent this from happening.");
		} finally {
			System.out.println(engine.stats());
		}
	}

	@Test
	public void shouldEvalGlobalClosuresEvenAfterEvictionOfClass() throws ScriptException {
		final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(2);
		final Graph g = TinkerFactory.createClassic();

		final Bindings bindings = engine.createBindings();
		bindings.put("g", g);

		// cache as global closure
		engine.eval("def isVadas(v){v.value('name')=='vadas'}", bindings);
		assertEquals(true, engine.eval("isVadas(g.v(2))", bindings));
		assertEquals(0, engine.stats().evictionCount());

		// evict
		engine.eval("def isMarko(v){v.value('name')=='marko'}", bindings);
		assertEquals(true, engine.eval("isMarko(g.v(1))", bindings));
		assertEquals(2, engine.stats().evictionCount());

		// isVadas class was evicted, but isVadas is still in the global closure cache
		assertEquals(true, engine.eval("isVadas(g.v(2))", bindings));
	}

	@Test
	public void shouldAllowFunctionsUsedInClosure() throws ScriptException {
		final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
		final Graph g = TinkerFactory.createClassic();

		final Bindings bindings = engine.createBindings();
		bindings.put("g", g);

		// this works on its own when the function and the line that uses it is in one "script".  this is the
		// current workaround
		assertEquals(g.v(2), engine.eval("def isVadas(v){v.value('name')=='vadas'};g.V.filter{isVadas(it)}.next()", bindings));

		// let's reset this piece and make sure isVadas is not hanging around.
		engine.reset();

		// validate that isVadas throws an exception since it is not defined
		try {
			engine.eval("isVadas(g.v(2))", bindings);

			// fail the test if the above doesn't throw an exception
			fail();
		} catch (Exception ex) {
			// this is good...we want this. it means isVadas isn't hanging about
		}

		// now...define the function separately on its own in one script
		engine.eval("def isVadas(v){v.value('name')=='vadas'}", bindings);

		// make sure the function works on its own...no problem
		assertEquals(true, engine.eval("isVadas(g.v(2))", bindings));

		// make sure the function works in a closure...this generates a StackOverflowError
		assertEquals(g.v(2), engine.eval("g.V.filter{isVadas(it)}.next()", bindings));
	}

	@Test
	@org.junit.Ignore
	public void shouldAllowUseOfClasses() throws ScriptException {
		GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
		final Graph g = TinkerFactory.createClassic();

		final Bindings bindings = engine.createBindings();
		bindings.put("g", g);

		// works when it's all defined together
		assertEquals(true, engine.eval("class c { static def isVadas(v){v.value('name')=='vadas'}};c.isVadas(g.v(2))", bindings));

		// let's reset this piece and make sure isVadas is not hanging around.
		engine.reset();

		// validate that isVadas throws an exception since it is not defined
		try {
			engine.eval("c.isVadas(g.v(2))", bindings);

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
		assertEquals(true, engine.eval("c.isVadas(g.v(2))", bindings));
	}

	@Test
	public void shouldCacheScriptsInLruFashion() throws Exception {
		final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(2);
		assertEquals(2, engine.eval("1+1"));
		assertEquals(3, engine.eval("1+2"));
		assertEquals(2, engine.stats().loadCount());
		assertEquals(2, engine.stats().missCount());
		assertEquals(0, engine.stats().hitCount());
		assertEquals(0, engine.stats().evictionCount());

		assertEquals(2, engine.eval("1+1"));
		assertEquals(3, engine.eval("1+2"));
		assertEquals(2, engine.stats().loadCount());
		assertEquals(2, engine.stats().missCount());
		assertEquals(2, engine.stats().hitCount());
		assertEquals(0, engine.stats().evictionCount());

		// since 1+1 was last touched cache should evict 1+2
		assertEquals(2, engine.eval("1+1"));
		assertEquals(4, engine.eval("1+3"));
		assertEquals(3, engine.stats().loadCount());
		assertEquals(3, engine.stats().missCount());
		assertEquals(3, engine.stats().hitCount());
		assertEquals(1, engine.stats().evictionCount());

		assertEquals(2, engine.eval("1+1"));
		assertEquals(4, engine.eval("1+3"));
		assertEquals(3, engine.stats().loadCount());
		assertEquals(3, engine.stats().missCount());
		assertEquals(5, engine.stats().hitCount());
		assertEquals(1, engine.stats().evictionCount());

		assertEquals(2, engine.eval("1+1"));
		assertEquals(3, engine.eval("1+2"));
		assertEquals(4, engine.stats().loadCount());
		assertEquals(4, engine.stats().missCount());
		assertEquals(6, engine.stats().hitCount());
		assertEquals(2, engine.stats().evictionCount());
	}

	@Test
	public void shouldBeOkToNotClearEngineScopeOnEviction() throws Exception {
		final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(1);
		engine.eval("x = { y -> y + 1}");
		Bindings b = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
		assertTrue(b.containsKey("x"));
		assertEquals(2, ((Closure) b.get("x")).call(1));

		// should evict the closure defined to x but it should remain accessible in the context
		assertEquals(2, engine.eval("x(1)"));
		b = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
		assertTrue(b.containsKey("x"));
		assertEquals(2, ((Closure) b.get("x")).call(1));

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
}
