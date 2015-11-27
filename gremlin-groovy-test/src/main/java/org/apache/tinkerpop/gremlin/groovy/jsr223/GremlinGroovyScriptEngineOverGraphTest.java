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

import groovy.lang.MissingPropertyException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.groovy.DefaultImportCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.NoImportCustomizerProvider;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;
import org.junit.Assert;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyScriptEngineOverGraphTest extends AbstractGremlinTest {

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldDoSomeGremlin() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine();
        final List list = new ArrayList();
        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("marko", convertToVertexId("marko"));
        bindings.put("temp", list);
        assertEquals(list.size(), 0);
        engine.eval("g.V(marko).out().fill(temp)",bindings);
        assertEquals(list.size(), 3);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldLoadImports() throws Exception {
        final ScriptEngine engineNoImports = new GremlinGroovyScriptEngine(NoImportCustomizerProvider.INSTANCE);
        try {
            engineNoImports.eval("Vertex.class.getName()");
            fail("Should have thrown an exception because no imports were supplied");
        } catch (Exception se) {
            assertTrue(se instanceof ScriptException);
        }

        final ScriptEngine engineWithImports = new GremlinGroovyScriptEngine(new DefaultImportCustomizerProvider());
        engineWithImports.put("g", g);
        assertEquals(Vertex.class.getName(), engineWithImports.eval("Vertex.class.getName()"));
        assertEquals(2l, engineWithImports.eval("g.V().has('age',gt(30)).count().next()"));
        assertEquals(Direction.IN, engineWithImports.eval("Direction.IN"));
        assertEquals(Direction.OUT, engineWithImports.eval("Direction.OUT"));
        assertEquals(Direction.BOTH, engineWithImports.eval("Direction.BOTH"));
    }


    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldLoadStandardImportsAndThenAddToThem() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(new DefaultImportCustomizerProvider());
        engine.put("g", g);
        assertEquals(Vertex.class.getName(), engine.eval("Vertex.class.getName()"));
        assertEquals(2l, engine.eval("g.V().has('age',gt(30)).count().next()"));
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
        assertEquals(2l, engine.eval("g.V().has('age',gt(30)).count().next()"));
        assertEquals(Direction.IN, engine.eval("Direction.IN"));
        assertEquals(Direction.OUT, engine.eval("Direction.OUT"));
        assertEquals(Direction.BOTH, engine.eval("Direction.BOTH"));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldProperlyHandleBindings() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine();
        engine.put("g", g);
        engine.put("marko", convertToVertexId("marko"));
        Assert.assertEquals(g.V(convertToVertexId("marko")).next(), engine.eval("g.V(marko).next()"));

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("s", "marko");
        bindings.put("f", 0.5f);
        bindings.put("i", 1);
        bindings.put("b", true);
        bindings.put("l", 100l);
        bindings.put("d", 1.55555d);

        assertEquals(engine.eval("g.E().has('weight',f).next()", bindings), g.E(convertToEdgeId("marko", "knows", "vadas")).next());
        assertEquals(engine.eval("g.V().has('name',s).next()", bindings), g.V(convertToVertexId("marko")).next());
        assertEquals(engine.eval("g.V().sideEffect{it.get().property('bbb',it.get().value('name')=='marko')}.iterate();g.V().has('bbb',b).next()", bindings), g.V(convertToVertexId("marko")).next());
        assertEquals(engine.eval("g.V().sideEffect{it.get().property('iii',it.get().value('name')=='marko'?1:0)}.iterate();g.V().has('iii',i).next()", bindings), g.V(convertToVertexId("marko")).next());
        assertEquals(engine.eval("g.V().sideEffect{it.get().property('lll',it.get().value('name')=='marko'?100l:0l)}.iterate();g.V().has('lll',l).next()", bindings), g.V(convertToVertexId("marko")).next());
        assertEquals(engine.eval("g.V().sideEffect{it.get().property('ddd',it.get().value('name')=='marko'?1.55555d:0)}.iterate();g.V().has('ddd',d).next()", bindings), g.V(convertToVertexId("marko")).next());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldClearBindingsBetweenEvals() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine();
        engine.put("g", g);
        engine.put("marko", convertToVertexId("marko"));
        assertEquals(g.V(convertToVertexId("marko")).next(), engine.eval("g.V(marko).next()"));

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("s", "marko");

        assertEquals(engine.eval("g.V().has('name',s).next()", bindings), g.V(convertToVertexId("marko")).next());

        try {
            engine.eval("g.V().has('name',s).next()");
            fail("This should have failed because s is no longer bound");
        } catch (Exception ex) {
            final Throwable t = ExceptionUtils.getRootCause(ex);
            assertEquals(MissingPropertyException.class, t.getClass());
            assertTrue(t.getMessage().startsWith("No such property: s for class"));
        }

    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldBeThreadSafe() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine();

        int runs = 500;
        final CountDownLatch latch = new CountDownLatch(runs);
        final List<String> names = Arrays.asList("marko", "peter", "josh", "vadas", "stephen", "pavel", "matthias");
        final Random random = new Random();

        for (int i = 0; i < runs; i++) {
            new Thread("test-thread-safe-" + i) {
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
                    } finally {
                        if (graph.features().graph().supportsTransactions())
                            g.tx().rollback();
                    }
                    latch.countDown();
                }
            }.start();
        }
        latch.await();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldBeThreadSafeOnCompiledScript() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        final CompiledScript script = engine.compile("t = g.V().has('name',name); if(t.hasNext()) { t } else { null }");

        int runs = 500;
        final CountDownLatch latch = new CountDownLatch(runs);
        final List<String> names = Arrays.asList("marko", "peter", "josh", "vadas", "stephen", "pavel", "matthias");
        final Random random = new Random();

        for (int i = 0; i < runs; i++) {
            new Thread("test-thread-safety-on-compiled-script-" + i) {
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
                    } finally {
                        if (graph.features().graph().supportsTransactions())
                            g.tx().rollback();
                    }
                    latch.countDown();
                }
            }.start();
        }
        latch.await();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldEvalGlobalClosuresEvenAfterEvictionOfClass() throws ScriptException {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("marko", convertToVertexId("marko"));
        bindings.put("vadas", convertToVertexId("vadas"));

        // strong referenced global closure
        engine.eval("def isVadas(v){v.value('name')=='vadas'}", bindings);
        assertEquals(true, engine.eval("isVadas(g.V(vadas).next())", bindings));

        // phantom referenced global closure
        bindings.put(GremlinGroovyScriptEngine.KEY_REFERENCE_TYPE, GremlinGroovyScriptEngine.REFERENCE_TYPE_PHANTOM);
        engine.eval("def isMarko(v){v.value('name')=='marko'}", bindings);

        try {
            engine.eval("isMarko(g.V(marko).next())", bindings);
            fail("the isMarko function should not be present");
        } catch (Exception ex) {

        }

        assertEquals(true, engine.eval("def isMarko(v){v.value('name')=='marko'}; isMarko(g.V(marko).next())", bindings));

        try {
            engine.eval("isMarko(g.V(marko"
            		+ ").next())", bindings);
            fail("the isMarko function should not be present");
        } catch (Exception ex) {

        }

        bindings.remove(GremlinGroovyScriptEngine.KEY_REFERENCE_TYPE);

        // isVadas class was a hard reference so it should still be hanging about
        assertEquals(true, engine.eval("isVadas(g.V(vadas).next())", bindings));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldAllowFunctionsUsedInClosure() throws ScriptException {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("#jsr223.groovy.engine.keep.globals", "phantom");
        bindings.put("vadas", convertToVertexId("vadas"));

        // this works on its own when the function and the line that uses it is in one "script".  this is the
        // current workaround
        assertEquals(g.V(convertToVertexId("vadas")).next(), engine.eval("def isVadas(v){v.value('name')=='vadas'};g.V().filter{isVadas(it.get())}.next()", bindings));

        // let's reset this piece and make sure isVadas is not hanging around.
        engine.reset();

        // validate that isVadas throws an exception since it is not defined
        try {
            engine.eval("isVadas(g.V(vadas).next())", bindings);

            // fail the test if the above doesn't throw an exception
            fail();
        } catch (Exception ex) {
            // this is good...we want this. it means isVadas isn't hanging about
        }

        // now...define the function separately on its own in one script
        bindings.remove("#jsr223.groovy.engine.keep.globals");
        engine.eval("def isVadas(v){v.value('name')=='vadas'}", bindings);

        // make sure the function works on its own...no problem
        assertEquals(true, engine.eval("isVadas(g.V(vadas).next())", bindings));

        // make sure the function works in a closure...this generates a StackOverflowError
        assertEquals(g.V(convertToVertexId("vadas")).next(), engine.eval("g.V().filter{isVadas(it.get())}.next()", bindings));
    }

    @Test
    @org.junit.Ignore
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldAllowUseOfClasses() throws ScriptException {
        GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("vadas", convertToVertexId("vadas"));

        // works when it's all defined together
        assertEquals(true, engine.eval("class c { static def isVadas(v){v.value('name')=='vadas'}};c.isVadas(g.V(vadas).next())", bindings));

        // let's reset this piece and make sure isVadas is not hanging around.
        engine.reset();

        // validate that isVadas throws an exception since it is not defined
        try {
            engine.eval("c.isVadas(g.V(vadas).next())", bindings);

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
        assertEquals(true, engine.eval("c.isVadas(g.V(vadas).next())", bindings));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldProcessUTF8Query() throws Exception {
        final Vertex nonUtf8 = graph.addVertex("name", "marko", "age", 29);
        final Vertex utf8Name = graph.addVertex("name", "轉注", "age", 32);

        final ScriptEngine engine = new GremlinGroovyScriptEngine();

        engine.put("g", g);
        Traversal eval = (Traversal) engine.eval("g.V().has('name', 'marko')");
        assertEquals(nonUtf8, eval.next());
        eval = (Traversal) engine.eval("g.V().has('name','轉注')");
        assertEquals(utf8Name, eval.next());
    }
}
