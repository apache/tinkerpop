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
import org.apache.tinkerpop.gremlin.groovy.CompilerCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.DefaultImportCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.NoImportCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
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
public class GremlinGroovyScriptEngineOverGraphTest {

    @Test
    public void shouldDoSomeGremlin() throws Exception {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final ScriptEngine engine = new GremlinGroovyScriptEngine();
        final List list = new ArrayList();
        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("marko", convertToVertexId(graph, "marko"));
        bindings.put("temp", list);
        assertEquals(list.size(), 0);
        engine.eval("g.V(marko).out().fill(temp)",bindings);
        assertEquals(list.size(), 3);
    }

    @Test
    public void shouldLoadImports() throws Exception {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final ScriptEngine engineNoImports = new GremlinGroovyScriptEngine((CompilerCustomizerProvider) NoImportCustomizerProvider.INSTANCE);
        try {
            engineNoImports.eval("Vertex.class.getName()");
            fail("Should have thrown an exception because no imports were supplied");
        } catch (Exception se) {
            assertTrue(se instanceof ScriptException);
        }

        final ScriptEngine engineWithImports = new GremlinGroovyScriptEngine((CompilerCustomizerProvider) new DefaultImportCustomizerProvider());
        engineWithImports.put("g", g);
        assertEquals(Vertex.class.getName(), engineWithImports.eval("Vertex.class.getName()"));
        assertEquals(2l, engineWithImports.eval("g.V().has('age',gt(30)).count().next()"));
        assertEquals(Direction.IN, engineWithImports.eval("Direction.IN"));
        assertEquals(Direction.OUT, engineWithImports.eval("Direction.OUT"));
        assertEquals(Direction.BOTH, engineWithImports.eval("Direction.BOTH"));
    }


    @Test
    public void shouldLoadStandardImportsAndThenAddToThem() throws Exception {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine((CompilerCustomizerProvider) new DefaultImportCustomizerProvider());
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
    public void shouldProperlyHandleBindings() throws Exception {
        final Graph graph = TinkerFactory.createClassic();
        final GraphTraversalSource g = graph.traversal();
        final ScriptEngine engine = new GremlinGroovyScriptEngine();
        engine.put("g", g);
        engine.put("marko", convertToVertexId(graph, "marko"));
        Assert.assertEquals(g.V(convertToVertexId(graph, "marko")).next(), engine.eval("g.V(marko).next()"));

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("s", "marko");
        bindings.put("f", 0.5f);
        bindings.put("i", 1);
        bindings.put("b", true);
        bindings.put("l", 100l);
        bindings.put("d", 1.55555d);

        assertEquals(engine.eval("g.E().has('weight',f).next()", bindings), g.E(convertToEdgeId(graph, "marko", "knows", "vadas")).next());
        assertEquals(engine.eval("g.V().has('name',s).next()", bindings), g.V(convertToVertexId(graph, "marko")).next());
        assertEquals(engine.eval("g.V().sideEffect{it.get().property('bbb',it.get().value('name')=='marko')}.iterate();g.V().has('bbb',b).next()", bindings), g.V(convertToVertexId(graph, "marko")).next());
        assertEquals(engine.eval("g.V().sideEffect{it.get().property('iii',it.get().value('name')=='marko'?1:0)}.iterate();g.V().has('iii',i).next()", bindings), g.V(convertToVertexId(graph, "marko")).next());
        assertEquals(engine.eval("g.V().sideEffect{it.get().property('lll',it.get().value('name')=='marko'?100l:0l)}.iterate();g.V().has('lll',l).next()", bindings), g.V(convertToVertexId(graph, "marko")).next());
        assertEquals(engine.eval("g.V().sideEffect{it.get().property('ddd',it.get().value('name')=='marko'?1.55555d:0)}.iterate();g.V().has('ddd',d).next()", bindings), g.V(convertToVertexId(graph, "marko")).next());
    }

    @Test
    public void shouldClearBindingsBetweenEvals() throws Exception {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final ScriptEngine engine = new GremlinGroovyScriptEngine();
        engine.put("g", g);
        engine.put("marko", convertToVertexId(graph, "marko"));
        assertEquals(g.V(convertToVertexId(graph, "marko")).next(), engine.eval("g.V(marko).next()"));

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("s", "marko");

        assertEquals(engine.eval("g.V().has('name',s).next()", bindings), g.V(convertToVertexId(graph, "marko")).next());

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
    public void shouldBeThreadSafe() throws Exception {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
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
    public void shouldBeThreadSafeOnCompiledScript() throws Exception {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
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
    public void shouldEvalGlobalClosuresEvenAfterEvictionOfClass() throws ScriptException {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("marko", convertToVertexId(graph, "marko"));
        bindings.put("vadas", convertToVertexId(graph, "vadas"));

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
    public void shouldAllowFunctionsUsedInClosure() throws ScriptException {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("#jsr223.groovy.engine.keep.globals", "phantom");
        bindings.put("vadas", convertToVertexId(graph, "vadas"));

        // this works on its own when the function and the line that uses it is in one "script".  this is the
        // current workaround
        assertEquals(g.V(convertToVertexId(graph, "vadas")).next(), engine.eval("def isVadas(v){v.value('name')=='vadas'};g.V().filter{isVadas(it.get())}.next()", bindings));

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
        assertEquals(g.V(convertToVertexId(graph, "vadas")).next(), engine.eval("g.V().filter{isVadas(it.get())}.next()", bindings));
    }

    @Test
    @org.junit.Ignore
    public void shouldAllowUseOfClasses() throws ScriptException {
        final Graph graph = TinkerFactory.createClassic();
        final GraphTraversalSource g = graph.traversal();
        GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("vadas", convertToVertexId(graph, "vadas"));

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
    public void shouldProcessUTF8Query() throws Exception {
        final Graph graph = TinkerGraph.open();
        final GraphTraversalSource g = graph.traversal();
        final Vertex nonUtf8 = graph.addVertex("name", "marko", "age", 29);
        final Vertex utf8Name = graph.addVertex("name", "轉注", "age", 32);

        final ScriptEngine engine = new GremlinGroovyScriptEngine();

        engine.put("g", g);
        Traversal eval = (Traversal) engine.eval("g.V().has('name', 'marko')");
        assertEquals(nonUtf8, eval.next());
        eval = (Traversal) engine.eval("g.V().has('name','轉注')");
        assertEquals(utf8Name, eval.next());
    }

    private Object convertToVertexId(final Graph graph, final String vertexName) {
        return convertToVertex(graph, vertexName).id();
    }

    private Vertex convertToVertex(final Graph graph, final String vertexName) {
        // all test graphs have "name" as a unique id which makes it easy to hardcode this...works for now
        return graph.traversal().V().has("name", vertexName).next();
    }

    private Object convertToEdgeId(final Graph graph, final String outVertexName, String edgeLabel, final String inVertexName) {
        return graph.traversal().V().has("name", outVertexName).outE(edgeLabel).as("e").inV().has("name", inVertexName).<Edge>select("e").next().id();
    }
}
