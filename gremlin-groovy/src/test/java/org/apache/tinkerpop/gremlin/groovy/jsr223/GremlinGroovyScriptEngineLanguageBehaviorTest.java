/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.groovy.jsr223;

import groovy.lang.GString;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.groovy.jsr223.ast.AmbiguousMethodASTTransformation;
import org.apache.tinkerpop.gremlin.groovy.jsr223.ast.RepeatASTTransformationCustomizer;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.junit.Before;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Pins down Groovy language behavior that scripts sent to the {@link GremlinGroovyScriptEngine} rely on: numeric
 * types, closures passed to steps, GString handling, global functions called from closures, the null-ambiguity
 * AST transformation, interrupts on loop forms other than {@code while}, compiler configuration and Grape. These are
 * the areas most likely to change silently with a new major version of Groovy, so the assertions are deliberately
 * exact (value and class).
 */
public class GremlinGroovyScriptEngineLanguageBehaviorTest {

    private GremlinGroovyScriptEngine engine;
    private Bindings modern;

    @Before
    public void setUp() {
        engine = new GremlinGroovyScriptEngine();
        modern = new SimpleBindings();
        modern.put("g", TinkerFactory.createModern().traversal());
    }

    // --- numbers ----------------------------------------------------------------------------------------------------

    @Test
    public void shouldKeepNumericLiteralTypes() throws Exception {
        assertValueAndType(1, Integer.class, "1");
        assertValueAndType(1L, Long.class, "1L");
        assertValueAndType(new BigDecimal("1.0"), BigDecimal.class, "1.0");
        assertValueAndType(1.0d, Double.class, "1.0d");
        assertValueAndType(1.0f, Float.class, "1.0f");
        assertValueAndType(BigInteger.ONE, BigInteger.class, "1g");
        assertValueAndType(new BigDecimal("1.5"), BigDecimal.class, "1.5g");
        assertValueAndType(2147483648L, Long.class, "2147483648");
    }

    @Test
    public void shouldKeepNumericOperatorResultTypes() throws Exception {
        assertValueAndType(new BigDecimal("0.5"), BigDecimal.class, "1/2");
        assertValueAndType(3, Integer.class, "10.intdiv(3)");
        assertValueAndType(1, Integer.class, "7 % 3");
        assertValueAndType(2L, Long.class, "1 + 1L");
        assertValueAndType(new BigDecimal("2.0"), BigDecimal.class, "1 + 1.0");
        assertValueAndType(2.0d, Double.class, "1 + 1.0d");
        assertValueAndType(6, Integer.class, "[1, 2, 3].sum()");
        // int arithmetic overflows rather than widening
        assertValueAndType(Integer.MIN_VALUE, Integer.class, "Integer.MAX_VALUE + 1");
    }

    @Test
    public void shouldKeepNumericTypesThroughATraversal() throws Exception {
        final List<?> result = (List<?>) engine.eval("g.inject(1, 1L, 1.0, 1.0d, 1.0f, 1g).toList()", modern);
        final List<Class<?>> types = new ArrayList<>();
        result.forEach(r -> types.add(r.getClass()));
        assertEquals(Arrays.asList(Integer.class, Long.class, BigDecimal.class, Double.class, Float.class,
                BigInteger.class), types);
        assertEquals(4L, engine.eval("g.V().hasLabel('person').count().next()", modern));
        assertValueAndType(123, Integer.class, "g.V().hasLabel('person').values('age').sum().next()", modern);
    }

    // --- closures passed to steps -----------------------------------------------------------------------------------

    @Test
    public void shouldUseClosureAsMapFunction() throws Exception {
        assertEquals(Arrays.asList("marko", "vadas", "lop", "josh", "ripple", "peter"),
                engine.eval("g.V().map{it.get().value('name')}.toList()", modern));
    }

    @Test
    public void shouldUseClosureAsFilterPredicate() throws Exception {
        assertEquals(Arrays.asList("josh", "peter"),
                engine.eval("g.V().filter{it.get().property('age').orElse(0) > 30}.values('name').toList()", modern));
    }

    @Test
    public void shouldUseClosureAsFlatMapFunction() throws Exception {
        assertEquals(3L, engine.eval("g.V().has('name','marko').flatMap{it.get().vertices(Direction.OUT)}.count().next()", modern));
    }

    @Test
    public void shouldUseClosureAsSideEffectConsumer() throws Exception {
        final List<Object> collected = new ArrayList<>();
        modern.put("collected", collected);
        engine.eval("g.V().hasLabel('software').sideEffect{collected << it.get().value('name')}.iterate()", modern);
        Collections.sort((List) collected);
        assertEquals(Arrays.asList("lop", "ripple"), collected);
    }

    @Test
    public void shouldUseOneArgumentClosureInByAsFunction() throws Exception {
        assertEquals(Arrays.asList("josh", "marko", "peter", "vadas"),
                engine.eval("g.V().hasLabel('person').order().by{it.value('name')}.values('name').toList()", modern));
    }

    @Test
    public void shouldUseTwoArgumentClosureInByAsComparator() throws Exception {
        assertEquals(Arrays.asList(35, 32, 29, 27),
                engine.eval("g.V().hasLabel('person').values('age').order().by{a, b -> b <=> a}.toList()", modern));
    }

    @Test
    public void shouldCoerceClosuresWithAs() throws Exception {
        assertEquals(Arrays.asList(32, 35),
                engine.eval("g.V().values('age').filter({it.get() > 30} as java.util.function.Predicate).order().toList()", modern));
        assertEquals(Arrays.asList("vadas", "peter", "marko", "josh"),
                engine.eval("g.V().hasLabel('person').values('name').order().by({a, b -> b <=> a} as Comparator).toList()", modern));
    }

    @Test
    public void shouldUseClosureAsSackSupplier() throws Exception {
        assertEquals(Arrays.asList(29, 27, 32, 35),
                engine.eval("g.withSack{0}.V().hasLabel('person').sack(sum).by('age').sack().toList()", modern));
    }

    @Test
    public void shouldUseClosureAsSideEffectSupplier() throws Exception {
        final Object result = engine.eval("g.withSideEffect('x', {[]}).V().hasLabel('software')" +
                ".sideEffect{it.sideEffects('x').add(it.get().value('name'))}.cap('x').next()", modern);
        final List<?> names = new ArrayList<>((List<?>) result);
        Collections.sort((List) names);
        assertEquals(Arrays.asList("lop", "ripple"), names);
    }

    // --- GString ----------------------------------------------------------------------------------------------------

    @Test
    public void shouldNotMatchStringPropertyValueWithGString() throws Exception {
        // surprising but current behavior: a GString is passed to has() as is, and a GString never equals a String,
        // so nothing matches unless it is converted with toString()
        assertEquals(0L, engine.eval("n = 'marko'; g.V().has('name', \"${n}\").count().next()", modern));
        assertEquals(1L, engine.eval("n = 'marko'; g.V().has('name', \"${n}\".toString()).count().next()", modern));
    }

    @Test
    public void shouldAcceptGStringAsPropertyKey() throws Exception {
        // String parameters (property keys) are coerced from GString
        assertEquals(Collections.singletonList(29),
                engine.eval("k = 'age'; g.V().has('name','marko').values(\"${k}\").toList()", modern));
    }

    @Test
    public void shouldReturnGStringForInterpolatedStrings() throws Exception {
        assertThat(engine.eval("x = 1; \"value ${x}\""), instanceOf(GString.class));
        assertEquals("value 1", engine.eval("x = 1; \"value ${x}\"").toString());
        // a plain double quoted string without interpolation stays a String
        assertThat(engine.eval("\"plain\""), instanceOf(String.class));
    }

    // --- global functions -------------------------------------------------------------------------------------------

    @Test
    public void shouldCallGlobalFunctionFromClosures() throws Exception {
        engine.eval("def plusOne(x) { x + 1 }");
        assertEquals(Arrays.asList(2, 3), engine.eval("[1, 2].collect{ plusOne(it) }"));
        assertEquals(Arrays.asList(Collections.singletonList(2), Collections.singletonList(3)),
                engine.eval("[[1], [2]].collect{ l -> l.collect{ plusOne(it) } }"));
        assertEquals(Arrays.asList("marko1", "vadas1"),
                engine.eval("g.V().hasLabel('person').limit(2).map{ plusOne(it.get().value('name')) }.toList()", modern));
    }

    @Test
    public void shouldReturnClassForScriptThatOnlyDeclaresAClass() throws Exception {
        final Object result = engine.eval("class Foo { int a }");
        assertThat(result, instanceOf(Class.class));
        assertEquals("Foo", ((Class<?>) result).getSimpleName());
    }

    // --- null ambiguity AST transformation --------------------------------------------------------------------------

    @Test
    public void shouldHandleInjectAmbiguousNull() throws Exception {
        final GremlinGroovyScriptEngine ambiguousNullEngine = new GremlinGroovyScriptEngine(
                (GroovyCustomizer) () -> new RepeatASTTransformationCustomizer(new AmbiguousMethodASTTransformation()));
        final Bindings b = new SimpleBindings();
        b.put("g", traversal().withEmbedded(EmptyGraph.instance()));
        assertEquals(Collections.singletonList(null), ambiguousNullEngine.eval("g.inject(null).toList()", b));
        assertEquals(Arrays.asList(1, null), ambiguousNullEngine.eval("g.inject(1, null).toList()", b));
    }

    @Test
    public void shouldCallInjectStepWithNullArgumentsWithoutTheAstTransformation() throws Exception {
        assertEquals(Collections.singletonList(null), engine.eval("g.inject(null).toList()", modern));
        assertEquals(Arrays.asList(null, null), engine.eval("g.inject(null, null).toList()", modern));
        assertEquals(Arrays.asList(null, 1), engine.eval("g.inject(1).inject(null).toList()", modern));
        assertEquals(Arrays.asList(null, 29), engine.eval("g.V(1).values('age').inject(null).toList()", modern));
        assertEquals(Arrays.asList(1, 2), engine.eval("g.inject(1, 2).toList()", modern));
    }

    // --- interrupts -------------------------------------------------------------------------------------------------

    @Test
    public void shouldTimeoutForLoop() throws Exception {
        assertTimedOut("for (;;) {}");
    }

    @Test
    public void shouldTimeoutClosureLoop() throws Exception {
        assertTimedOut("(1..Integer.MAX_VALUE).each { it }");
    }

    @Test
    public void shouldInterruptForLoop() throws Exception {
        assertThreadInterrupted("for (;;) {}");
    }

    @Test
    public void shouldInterruptClosureLoop() throws Exception {
        assertThreadInterrupted("(1..Integer.MAX_VALUE).each { it }");
    }

    // --- compiler configuration and Grape ---------------------------------------------------------------------------

    @Test
    public void shouldApplyCompilerConfigurationSettingsUsedInConfiguration() throws Exception {
        final CompilerConfiguration configuration = new CompilerConfiguration();
        new ConfigurationGroovyCustomizer("TargetBytecode", "17").applyCustomization(configuration);
        assertEquals("17", configuration.getTargetBytecode());

        final GremlinGroovyScriptEngine configured = new GremlinGroovyScriptEngine(
                new ConfigurationGroovyCustomizer("TargetBytecode", "17"));
        assertEquals(2, configured.eval("1 + 1"));
    }

    @Test
    public void shouldHaveGrapeAvailable() throws Exception {
        // :install in the console and "gremlin-server.sh install" depend on Grape and its Ivy integration
        final Class<?> grape = Class.forName("groovy.grape.Grape");
        assertNotNull(grape.getMethod("getInstance").invoke(null));
    }

    @Test
    public void shouldEmptyClassCacheOnReset() throws Exception {
        for (int ix = 0; ix < 20; ix++) {
            engine.eval("1 + " + ix);
        }
        assertTrue(engine.getClassCacheEstimatedSize() > 0);
        engine.reset();
        assertEquals(0, engine.getClassCacheEstimatedSize());
        assertEquals(2, engine.eval("1 + 1"));
    }

    // --- helpers ----------------------------------------------------------------------------------------------------

    private void assertValueAndType(final Object expected, final Class<?> type, final String script) throws ScriptException {
        assertValueAndType(expected, type, script, new SimpleBindings());
    }

    private void assertValueAndType(final Object expected, final Class<?> type, final String script,
                                    final Bindings bindings) throws ScriptException {
        final Object actual = engine.eval(script, bindings);
        assertEquals(script, expected, actual);
        assertEquals(script, type, actual.getClass());
    }

    private static void assertTimedOut(final String script) {
        final GremlinGroovyScriptEngine timed = new GremlinGroovyScriptEngine(new TimedInterruptGroovyCustomizer(500));
        try {
            timed.eval(script);
            fail("should have timed out: " + script);
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(script, root, instanceOf(TimedInterruptTimeoutException.class));
        }
    }

    private static void assertThreadInterrupted(final String script) throws Exception {
        final GremlinGroovyScriptEngine interruptible = new GremlinGroovyScriptEngine(new ThreadInterruptGroovyCustomizer());
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread t = new Thread(() -> {
            try {
                interruptible.eval(script);
            } catch (Exception ex) {
                failure.set(ExceptionUtils.getRootCause(ex));
            }
        });
        t.start();
        Thread.sleep(200);
        t.interrupt();
        t.join(10000);
        assertTrue("script was not interrupted: " + script, !t.isAlive());
        assertThat(script, failure.get(), instanceOf(InterruptedException.class));
    }
}
