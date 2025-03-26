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
import groovy.lang.MissingMethodException;
import groovy.lang.MissingPropertyException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.groovy.jsr223.ast.AmbiguousMethodASTTransformation;
import org.apache.tinkerpop.gremlin.groovy.jsr223.ast.RepeatASTTransformationCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.javatuples.Pair;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
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
    private static final Object[] EMPTY_ARGS = new Object[0];

    private static final GremlinGroovyScriptEngine ambiguousNullEngine = new GremlinGroovyScriptEngine(
            (GroovyCustomizer) () -> new RepeatASTTransformationCustomizer(new AmbiguousMethodASTTransformation()));

    @Test
    public void shouldNotCacheGlobalFunctions() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(CompilationOptionsCustomizer.build().
                enableGlobalFunctionCache(false).create());

        assertEquals(3, engine.eval("def addItUp(x,y){x+y};addItUp(1,2)"));

        try {
            engine.eval("addItUp(1,2)");
            fail("Global functions should not be cached so the call to addItUp() should fail");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(MissingMethodException.class));
        }
    }

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
        engine.eval("def addItUp(x,y){x+y}");
        assertEquals(3, engine.eval("1+2"));
        assertEquals(3, engine.eval("addItUp(1,2)"));
    }

    @Test
    public void shouldPromoteDefinedVarsInInterpreterModeWithNoBindings() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(new InterpreterModeGroovyCustomizer());
        engine.eval("def addItUp = { x, y -> x + y }");
        assertEquals(3, engine.eval("int xxx = 1 + 2"));
        assertEquals(4, engine.eval("yyy = xxx + 1"));
        assertEquals(7, engine.eval("def zzz = yyy + xxx"));
        assertEquals(4, engine.eval("zzz - xxx"));
        assertEquals("accessible-globally", engine.eval("if (yyy > 0) { def inner = 'should-stay-local'; outer = 'accessible-globally' }\n outer"));
        assertEquals("accessible-globally", engine.eval("outer"));

        try {
            engine.eval("inner");
            fail("Should not have been able to access 'inner'");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(MissingPropertyException.class));
        }

        assertEquals(10, engine.eval("addItUp(zzz,xxx)"));
    }

    @Test
    public void shouldPromoteDefinedVarsInInterpreterModeWithBindings() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(new InterpreterModeGroovyCustomizer());
        final Bindings b = new SimpleBindings();
        b.put("x", 2);
        engine.eval("def addItUp = { x, y -> x + y }", b);
        assertEquals(3, engine.eval("int xxx = 1 + x", b));
        assertEquals(4, engine.eval("yyy = xxx + 1", b));
        assertEquals(7, engine.eval("def zzz = yyy + xxx", b));
        assertEquals(4, engine.eval("zzz - xxx", b));
        assertEquals("accessible-globally", engine.eval("if (yyy > 0) { def inner = 'should-stay-local'; outer = 'accessible-globally' }\n outer", b));
        assertEquals("accessible-globally", engine.eval("outer", b));

        try {
            engine.eval("inner", b);
            fail("Should not have been able to access 'inner'");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(MissingPropertyException.class));
        }

        assertEquals(10, engine.eval("addItUp(zzz,xxx)", b));
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
            // do nothing = expected
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
    public void shouldResetClassLoader() throws Exception {
        final GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine();
        try {
            scriptEngine.eval("addOne(1)");
            fail("Should have tossed ScriptException since addOne is not yet defined.");
        } catch (ScriptException se) {
            // do nothing = expected
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
            // do nothing = expected
        }
    }

    @Test
    public void shouldProcessScriptWithUTF8Characters() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine();
        assertEquals("轉注", engine.eval("'轉注'"));
    }

    @Test
    public void shouldAllowVariableReuseAcrossThreads() throws Exception {
        final BasicThreadFactory testingThreadFactory = new BasicThreadFactory.Builder().namingPattern("test-gremlin-scriptengine-%d").build();
        final ExecutorService service = Executors.newFixedThreadPool(8, testingThreadFactory);
        final GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine();

        final AtomicBoolean failed = new AtomicBoolean(false);
        final int max = 512;
        final List<Pair<Integer, List<Integer>>> futures = Collections.synchronizedList(new ArrayList<>(max));
        IntStream.range(0, max).forEach(i -> {
            final int yValue = i * 2;
            final int zValue = i * -1;
            final Bindings b = new SimpleBindings();
            b.put("x", i);
            b.put("y", yValue);

            final String script = "z=" + zValue + ";[x,y,z]";
            try {
                service.submit(() -> {
                    try {
                        final List<Integer> result = (List<Integer>) scriptEngine.eval(script, b);
                        futures.add(Pair.with(i, result));
                    } catch (Exception ex) {
                        failed.set(true);
                    }
                });
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        service.shutdown();
        assertThat(service.awaitTermination(120000, TimeUnit.MILLISECONDS), is(true));

        // likely a concurrency exception if it occurs - and if it does then we've messed up because that's what this
        // test is partially designed to protected against.
        assertThat(failed.get(), is(false));
        assertEquals(max, futures.size());
        futures.forEach(t -> {
            assertEquals(t.getValue0(), t.getValue1().get(0));
            assertEquals(t.getValue0() * 2, t.getValue1().get(1).intValue());
            assertEquals(t.getValue0() * -1, t.getValue1().get(2).intValue());
        });
    }

    @Test
    public void shouldInvokeFunctionRedirectsOutputToContextWriter() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        StringWriter writer = new StringWriter();
        engine.getContext().setWriter(writer);

        final String code = "def myFunction() { print \"Hello World!\" }";
        engine.eval(code);
        engine.invokeFunction("myFunction", EMPTY_ARGS);
        assertEquals("Hello World!", writer.toString());

        writer = new StringWriter();
        final StringWriter writer2 = new StringWriter();
        engine.getContext().setWriter(writer2);
        engine.invokeFunction("myFunction", EMPTY_ARGS);
        assertEquals("", writer.toString());
        assertEquals("Hello World!", writer2.toString());
    }

    @Test
    public void shouldInvokeFunctionRedirectsOutputToContextOut() throws Exception {
        final GremlinGroovyScriptEngine  engine = new GremlinGroovyScriptEngine();
        StringWriter writer = new StringWriter();
        final StringWriter unusedWriter = new StringWriter();
        engine.getContext().setWriter(unusedWriter);
        engine.put("out", writer);

        final String code = "def myFunction() { print \"Hello World!\" }";
        engine.eval(code);
        engine.invokeFunction("myFunction", EMPTY_ARGS);
        assertEquals("", unusedWriter.toString());
        assertEquals("Hello World!", writer.toString());

        writer = new StringWriter();
        final StringWriter writer2 = new StringWriter();
        engine.put("out", writer2);
        engine.invokeFunction("myFunction", EMPTY_ARGS);
        assertEquals("", unusedWriter.toString());
        assertEquals("", writer.toString());
        assertEquals("Hello World!", writer2.toString());
    }

    @Test
    public void shouldEnableEngineContextAccessibleToScript() throws Exception {
        final GremlinGroovyScriptEngine  engine = new GremlinGroovyScriptEngine();
        final ScriptContext engineContext = engine.getContext();
        engine.put("theEngineContext", engineContext);
        final String code = "[answer: theEngineContext.is(context)]";
        assertThat(((Map) engine.eval(code)).get("answer"), is(true));
    }

    @Test
    public void shouldEnableContextBindingOverridesEngineContext() throws Exception {
        final GremlinGroovyScriptEngine  engine = new GremlinGroovyScriptEngine();
        final ScriptContext engineContext = engine.getContext();
        final Map<String,Object> otherContext = new HashMap<>();
        otherContext.put("foo", "bar");
        engine.put("context", otherContext);
        engine.put("theEngineContext", engineContext);
        final String code = "[answer: context.is(theEngineContext) ? \"wrong\" : context.foo]";
        assertEquals("bar", ((Map) engine.eval(code)).get("answer"));
    }

    @Test
    public void shouldGetClassMapCacheBasicStats() throws Exception {
        final GremlinGroovyScriptEngine  engine = new GremlinGroovyScriptEngine();
        assertEquals(0, engine.getClassCacheEstimatedSize());
        assertEquals(0, engine.getClassCacheHitCount());
        assertEquals(0, engine.getClassCacheLoadCount());
        assertEquals(0, engine.getClassCacheLoadFailureCount());
        assertEquals(0, engine.getClassCacheLoadSuccessCount());

        engine.eval("1+1");

        assertEquals(1, engine.getClassCacheEstimatedSize());
        assertEquals(0, engine.getClassCacheHitCount());
        assertEquals(1, engine.getClassCacheLoadCount());
        assertEquals(0, engine.getClassCacheLoadFailureCount());
        assertEquals(1, engine.getClassCacheLoadSuccessCount());

        for (int ix = 0; ix < 100; ix++) {
            engine.eval("1+1");
        }

        assertEquals(1, engine.getClassCacheEstimatedSize());
        assertEquals(100, engine.getClassCacheHitCount());
        assertEquals(1, engine.getClassCacheLoadCount());
        assertEquals(0, engine.getClassCacheLoadFailureCount());
        assertEquals(1, engine.getClassCacheLoadSuccessCount());

        for (int ix = 0; ix < 100; ix++) {
            engine.eval("1+" + ix);
        }

        assertEquals(100, engine.getClassCacheEstimatedSize());
        assertEquals(101, engine.getClassCacheHitCount());
        assertEquals(100, engine.getClassCacheLoadCount());
        assertEquals(0, engine.getClassCacheLoadFailureCount());
        assertEquals(100, engine.getClassCacheLoadSuccessCount());

        try {
            engine.eval("(me broken");
            fail("Should have tanked with compilation error");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(ScriptException.class));
        }

        assertEquals(101, engine.getClassCacheEstimatedSize());
        assertEquals(101, engine.getClassCacheHitCount());
        assertEquals(101, engine.getClassCacheLoadCount());
        assertEquals(1, engine.getClassCacheLoadFailureCount());
        assertEquals(100, engine.getClassCacheLoadSuccessCount());

        try {
            engine.eval("(me broken");
            fail("Should have tanked with compilation error");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(ScriptException.class));
        }

        assertEquals(101, engine.getClassCacheEstimatedSize());
        assertEquals(102, engine.getClassCacheHitCount());
        assertEquals(101, engine.getClassCacheLoadCount());
        assertEquals(1, engine.getClassCacheLoadFailureCount());
        assertEquals(100, engine.getClassCacheLoadSuccessCount());
    }

    @Test
    public void shouldEvalForLambda() throws Exception {
        // https://issues.apache.org/jira/browse/TINKERPOP-1953
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        final Lambda l = (Lambda) engine.eval(" org.apache.tinkerpop.gremlin.util.function.Lambda.function(\"{ it.get() }\")");
        assertEquals("{ it.get() }", l.getLambdaScript());
    }

    @Test
    public void shouldAllowGroovySyntaxForStrategies() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        final GraphTraversalSource g = EmptyGraph.instance().traversal();

        final Bindings b = new SimpleBindings();
        b.put("g", g);

        Traversal t = (Traversal) engine.eval("g.withStrategies(ReadOnlyStrategy).V()", b);
        Optional<ReadOnlyStrategy> ro = t.asAdmin().getStrategies().getStrategy(ReadOnlyStrategy.class);
        assertThat(ro.isPresent(), is(true));
        assertEquals(ReadOnlyStrategy.instance(), ro.get());

        t = (Traversal) engine.eval("g.withStrategies(new SubgraphStrategy(vertices: __.hasLabel(\"person\"))).V()", b);
        Optional<SubgraphStrategy> ss = t.asAdmin().getStrategies().getStrategy(SubgraphStrategy.class);
        assertThat(ss.isPresent(), is(true));
        assertEquals(HasStep.class, ss.get().getVertexCriterion().asAdmin().getStartStep().getClass());

        t = (Traversal) engine.eval("g.withStrategies(ReadOnlyStrategy, new SubgraphStrategy(vertices: __.hasLabel(\"person\"))).V()", b);
        ro = t.asAdmin().getStrategies().getStrategy(ReadOnlyStrategy.class);
        assertThat(ro.isPresent(), is(true));
        assertEquals(ReadOnlyStrategy.instance(), ro.get());
        ss = t.asAdmin().getStrategies().getStrategy(SubgraphStrategy.class);
        assertThat(ss.isPresent(), is(true));
        assertEquals(HasStep.class, ss.get().getVertexCriterion().asAdmin().getStartStep().getClass());
    }

    /**
     * Test for TINKERPOP-2394 Unable to use __ class of a custom DSL when passing a script even if this class is imported
     */
	@Test
	public void customizerShouldOverrideCoreImports() throws Exception {
		DefaultImportCustomizer customizer = DefaultImportCustomizer.build()
				.addClassImports(org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.__.class)
				.create();
		final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(customizer);
		engine.eval("__.users();[]");
	}

    @Test
    public void shouldHandleMergeVAmbiguousNull() throws Exception {
        final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());
        final Bindings bindings = new SimpleBindings();
        bindings.put("g", g);
        ambiguousNullEngine.eval("g.mergeV(null)", bindings);
        ambiguousNullEngine.eval("g.mergeV(null).option(Merge.onCreate, null)", bindings);
        ambiguousNullEngine.eval("g.mergeV([:]).option(Merge.onCreate, null)", bindings);
    }

    @Test
    public void shouldHandleMergeEAmbiguousNull() throws Exception {
        final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());
        final Bindings bindings = new SimpleBindings();
        bindings.put("g", g);
        ambiguousNullEngine.eval("g.mergeE(null)", bindings);
        ambiguousNullEngine.eval("g.mergeE(null).option(Merge.onCreate, null)", bindings);
        ambiguousNullEngine.eval("g.mergeE([:]).option(Merge.onCreate, null)", bindings);
    }

    @Test
    public void shouldHandleHasIdAmbiguousNull() throws Exception {
        final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());
        final Bindings bindings = new SimpleBindings();
        bindings.put("g", g);
        ambiguousNullEngine.eval("g.V().hasId(null)", bindings);
        ambiguousNullEngine.eval("g.V().hasId(P.eq(1), null)", bindings);
    }

    /**
     * Reproducer for TINKERPOP-2953.
     */
    @Test
    public void shouldBeAbleToCallStaticallyImportedValuesMethodWithArgument() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        final Object values = engine.eval("values('a')");
        assertTrue(values instanceof GraphTraversal);
    }

    @Test
    public void shouldBeAbleToCallStaticallyImportedValuesMethodWithoutArguments() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        engine.eval("values()");
        // values() is evaluated ambiguously by Groovy and could either be Column.values() or __.values()
        // so assume it works if no "groovy.lang.MissingMethodException: No signature of method" thrown.
    }

    @Test
    public void shouldBeAbleToCallColumnEnumConstantValues() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        final Object values = engine.eval("values");
        assertEquals(Column.values, values);
    }

    public static class TestStrategy<S extends TraversalStrategy> extends AbstractTraversalStrategy<S> {
        private final Configuration configuration;

        public TestStrategy(final Map configuration) {
            this(new MapConfiguration(configuration));
        }

        private TestStrategy(final Configuration configuration) {
            this.configuration = configuration;
        }
        @Override
        public void apply(Traversal.Admin traversal) {
            // Do nothing
        }

        @Override
        public Configuration getConfiguration() {
            return configuration;
        }

        public static TestStrategy create(Configuration configuration) {
            return new TestStrategy(configuration);
        }
    }

    @Test
    public void shouldReconstructCustomRegisteredStrategy() throws ScriptException {
        GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine(DefaultImportCustomizer.build().addClassImports(TestStrategy.class).create());
        final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());
        final Bindings bindings = new SimpleBindings();
        bindings.put("g", g);

        GraphTraversal traversal = (GraphTraversal) scriptEngine.eval("g.withStrategies(new TestStrategy(stringKey:\"stringValue\",intKey:1,booleanKey:true)).V()", bindings);

        TestStrategy reconstructedStrategy = traversal.asAdmin().getStrategies().getStrategy(TestStrategy.class).get();

        assertNotNull(reconstructedStrategy);

        MapConfiguration expectedConfig = new MapConfiguration(new HashMap<String, Object>() {{
            put("stringKey", "stringValue");
            put("intKey", 1);
            put("booleanKey", true);
        }});

        Set<String> expectedKeys = new HashSet<>();
        Set<String> actualKeys = new HashSet<>();
        expectedConfig.getKeys().forEachRemaining((key) -> expectedKeys.add(key));
        reconstructedStrategy.getConfiguration().getKeys().forEachRemaining((key) -> actualKeys.add(key));

        assertEquals(expectedKeys, actualKeys);

        expectedKeys.forEach((key) -> {
            assertEquals(expectedConfig.get(Object.class, key), reconstructedStrategy.getConfiguration().get(Object.class, key));
        });
    }
}