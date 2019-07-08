/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.groovy.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.TranslatorCustomizer;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategy;
import org.apache.tinkerpop.gremlin.groovy.jsr223.ParameterizedGroovyTranslator.ParameterizedGroovyResult;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Function;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stark Arya (sandszhou.zj@alibaba-inc.com)
 * @author Stark Arya (sandszhou.zj@alibaba-inc.com)
 */
public class ParameterizedGroovyTranslatorTest {

    private Graph graph = TinkerGraph.open();
    private GraphTraversalSource g = graph.traversal();
    private static final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

    @Test
    public void shouldSupportStringSupplierLambdas() {
        final TinkerGraph graph = TinkerFactory.createModern();
        GraphTraversalSource g = graph.traversal();
        g = g.withStrategies(new TranslationStrategy(g, ParameterizedGroovyTranslator.of("g"), false));
        final GraphTraversal.Admin<Vertex, Integer> t = g.withSideEffect("lengthSum", 0).withSack(1)
                .V()
                .filter(Lambda.predicate("it.get().label().equals('person')"))
                .flatMap(Lambda.function("it.get().vertices(Direction.OUT)"))
                .map(Lambda.<Traverser<Object>, Integer>function("it.get().value('name').length()"))
                .sideEffect(Lambda.consumer("{ x -> x.sideEffects(\"lengthSum\", x.<Integer>sideEffects('lengthSum') + x.get()) }"))
                .order().by(Lambda.comparator("a,b -> a <=> b"))
                .sack(Lambda.biFunction("{ a,b -> a + b }"))
                .asAdmin();

        final List<Integer> sacks = new ArrayList<>();
        final List<Integer> lengths = new ArrayList<>();
        while (t.hasNext()) {
            final Traverser.Admin<Integer> traverser = t.nextTraverser();
            sacks.add(traverser.sack());
            lengths.add(traverser.get());
        }
        assertFalse(t.hasNext());
        //
        assertEquals(6, lengths.size());
        assertEquals(3, lengths.get(0).intValue());
        assertEquals(3, lengths.get(1).intValue());
        assertEquals(3, lengths.get(2).intValue());
        assertEquals(4, lengths.get(3).intValue());
        assertEquals(5, lengths.get(4).intValue());
        assertEquals(6, lengths.get(5).intValue());
        ///
        assertEquals(6, sacks.size());
        assertEquals(4, sacks.get(0).intValue());
        assertEquals(4, sacks.get(1).intValue());
        assertEquals(4, sacks.get(2).intValue());
        assertEquals(5, sacks.get(3).intValue());
        assertEquals(6, sacks.get(4).intValue());
        assertEquals(7, sacks.get(5).intValue());
        //
        assertEquals(24, t.getSideEffects().<Number>get("lengthSum").intValue());

        final ParameterizedGroovyResult parameterizedScript = ParameterizedGroovyTranslator.of("g").parameterizedTranslate(t.getBytecode());
        assertEquals(9, parameterizedScript.getParameters().size());
        assertEquals("lengthSum", parameterizedScript.getParameters().get("_args_0"));
        assertEquals(Integer.valueOf(0), parameterizedScript.getParameters().get("_args_1"));
        assertEquals(Integer.valueOf(1), parameterizedScript.getParameters().get("_args_2"));
        assertEquals(Lambda.predicate("it.get().label().equals('person')"), parameterizedScript.getParameters().get("_args_3"));
        assertEquals(Lambda.function("it.get().vertices(Direction.OUT)"), parameterizedScript.getParameters().get("_args_4"));
        assertEquals(Lambda.<Traverser<Object>, Integer>function("it.get().value('name').length()"), parameterizedScript.getParameters().get("_args_5"));
        assertEquals(Lambda.consumer("{ x -> x.sideEffects(\"lengthSum\", x.<Integer>sideEffects('lengthSum') + x.get()) }"), parameterizedScript.getParameters().get("_args_6"));
        assertEquals(Lambda.comparator("a,b -> a <=> b"), parameterizedScript.getParameters().get("_args_7"));
        assertEquals(Lambda.biFunction("{ a,b -> a + b }"), parameterizedScript.getParameters().get("_args_8"));
        assertEquals("g.withStrategies(org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategy.instance())" +
                        ".withSideEffect(_args_0,_args_1).withSack(_args_2)" +
                        ".V()" +
                        ".filter(_args_3)" +
                        ".flatMap(_args_4)" +
                        ".map(_args_5)" +
                        ".sideEffect(_args_6)" +
                        ".order().by(_args_7)" +
                        ".sack(_args_8)",
                parameterizedScript.getScript());
    }

    @Test
    public void shouldHandleMaps() {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final ParameterizedGroovyResult parameterizedScript = ParameterizedGroovyTranslator.of("g").parameterizedTranslate(g.V().id().is(new LinkedHashMap<Object,Object>() {{
            put(3, "32");
            put(Arrays.asList(1, 2, 3.1d), 4);
        }}).asAdmin().getBytecode());

        assertEquals(6, parameterizedScript.getParameters().size());
        assertEquals(Integer.valueOf(3), parameterizedScript.getParameters().get("_args_0"));
        assertEquals("32", parameterizedScript.getParameters().get("_args_1"));
        assertEquals(Integer.valueOf(1), parameterizedScript.getParameters().get("_args_2"));
        assertEquals(Integer.valueOf(2), parameterizedScript.getParameters().get("_args_3"));
        assertEquals(Double.valueOf(3.1), parameterizedScript.getParameters().get("_args_4"));
        assertEquals(Integer.valueOf(4), parameterizedScript.getParameters().get("_args_5"));
        assertEquals("g.V().id().is([(_args_0):(_args_1),([_args_2, _args_3, _args_4]):(_args_5)])", parameterizedScript.getScript());
        final String standardScript = GroovyTranslator.of("g").translate(g.V().id().is(new LinkedHashMap<Object,Object>() {{
            put(3, "32");
            put(Arrays.asList(1, 2, 3.1d), 4);
        }}).asAdmin().getBytecode());
        final Bindings bindings = new SimpleBindings();
        bindings.putAll(parameterizedScript.getParameters());
        bindings.put("g", g);
        assertParameterizedScriptOk(standardScript, parameterizedScript.getScript(), bindings);
    }

    @Test
    public void shouldHandleEmptyMaps() {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final Function identity = new Lambda.OneArgLambda("it.get()", "gremlin-groovy");
        final ParameterizedGroovyResult parameterizedScript = ParameterizedGroovyTranslator.of("g").parameterizedTranslate(g.inject(Collections.emptyMap()).map(identity).asAdmin().getBytecode());

        assertEquals(1, parameterizedScript.getParameters().size());
        assertEquals(identity, parameterizedScript.getParameters().get("_args_0"));
        assertEquals("g.inject([]).map(_args_0)", parameterizedScript.getScript());
        final Bindings bindings = new SimpleBindings();
        bindings.putAll(parameterizedScript.getParameters());
        bindings.put("g", g);
        assertThatScriptOk(parameterizedScript.getScript(), bindings);
    }

    @Test
    public void shouldHandleDate() {
        final Calendar c = Calendar.getInstance();
        c.set(1975, Calendar.SEPTEMBER, 7);
        final Date d = c.getTime();
        assertParameterizedTranslation(String.format("new java.util.Date(%s)", d.getTime()), d);
    }

    @Test
    public void shouldHandleTimestamp() {
        final Calendar c = Calendar.getInstance();
        c.set(1975, Calendar.SEPTEMBER, 7);
        final Timestamp t = new Timestamp(c.getTime().getTime());
        assertParameterizedTranslation(String.format("new java.sql.Timestamp(%s)", t.getTime()), t);
    }

    @Test
    public void shouldHandleUuid() {
        final UUID uuid = UUID.fromString("ffffffff-fd49-1e4b-0000-00000d4b8a1d");
        assertParameterizedTranslation(String.format("java.util.UUID.fromString('%s')", uuid), uuid);
    }

    @Test
    public void shouldHandleColumn() {
        assertNonParameterizedTranslation("Column.keys", Column.keys);
    }

    @Test
    public void shouldHandleDirection() {
        assertNonParameterizedTranslation("Direction.BOTH", Direction.BOTH);
    }

    @Test
    public void shouldHandleOrder() {
        assertNonParameterizedTranslation("Order.decr", Order.decr);
    }

    @Test
    public void shouldHandlePop() {
        assertNonParameterizedTranslation("Pop.last", Pop.last);
    }

    @Test
    public void shouldHandleScope() {
        assertNonParameterizedTranslation("Scope.local", Scope.local);
    }

    @Test
    public void shouldIncludeCustomTypeTranslationForSomethingSilly() throws Exception {
        final TinkerGraph graph = TinkerGraph.open();
        final ParameterizedSillyClass notSillyEnough = ParameterizedSillyClass.from("not silly enough", 100);
        final GraphTraversalSource g = graph.traversal();

        // without type translation we get uglinesss
        final ParameterizedGroovyResult parameterizedScriptBad = ParameterizedGroovyTranslator.of("g").
                parameterizedTranslate(g.inject(notSillyEnough).asAdmin().getBytecode());
        assertEquals(String.format("g.inject(%s)", "_args_0"), parameterizedScriptBad.getScript());
        assertEquals(notSillyEnough, parameterizedScriptBad.getParameters().get("_args_0"));

        // with type translation we get valid gremlin
        final String parameterizedScriptGood = ParameterizedGroovyTranslator.of("g", new ParameterizedSillyClassTranslator()).
                translate(g.inject(notSillyEnough).asAdmin().getBytecode());
        assertEquals(String.format("g.inject(org.apache.tinkerpop.gremlin.groovy.jsr223.ParameterizedGroovyTranslatorTest.ParameterizedSillyClass.from('%s', (int) %s))", notSillyEnough.getX(), notSillyEnough.getY()), parameterizedScriptGood);
        assertThatScriptOk(parameterizedScriptGood, "g", g);

        final GremlinGroovyScriptEngine customEngine = new GremlinGroovyScriptEngine(new ParameterizedSillyClassTranslatorCustomizer());
        final Bindings b = new SimpleBindings();
        b.put("g", g);
        final Traversal t = customEngine.eval(g.inject(notSillyEnough).asAdmin().getBytecode(), b, "g");
        final ParameterizedSillyClass sc = (ParameterizedSillyClass) t.next();
        assertEquals(notSillyEnough.getX(), sc.getX());
        assertEquals(notSillyEnough.getY(), sc.getY());
        assertThat(t.hasNext(), is(false));
    }

    @Test
    public void shouldHaveValidToString() {
        assertEquals("translator[h:gremlin-groovy]", GroovyTranslator.of("h").toString());
    }

    @Test
    public void shouldEscapeStrings() {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final ParameterizedGroovyResult parameterizedScript = ParameterizedGroovyTranslator.of("g").parameterizedTranslate(g.addV("customer")
                .property("customer_id", 501L)
                .property("name", "Foo\u0020Bar")
                .property("age", 25)
                .property("special", "`~!@#$%^&*()-_=+[{]}\\|;:'\",<.>/?")
                .asAdmin().getBytecode());

        assertEquals(9, parameterizedScript.getParameters().size());
        assertEquals("customer", parameterizedScript.getParameters().get("_args_0"));
        assertEquals("customer_id", parameterizedScript.getParameters().get("_args_1"));
        assertEquals(Long.valueOf(501), parameterizedScript.getParameters().get("_args_2"));
        assertEquals("name", parameterizedScript.getParameters().get("_args_3"));
        assertEquals("Foo\u0020Bar", parameterizedScript.getParameters().get("_args_4"));
        assertEquals("age", parameterizedScript.getParameters().get("_args_5"));
        assertEquals(Integer.valueOf(25), parameterizedScript.getParameters().get("_args_6"));
        assertEquals("special", parameterizedScript.getParameters().get("_args_7"));
        assertEquals("`~!@#$%^&*()-_=+[{]}\\|;:'\",<.>/?", parameterizedScript.getParameters().get("_args_8"));
        assertEquals("g.addV(_args_0).property(_args_1,_args_2).property(_args_3,_args_4).property(_args_5,_args_6).property(_args_7,_args_8)", parameterizedScript.getScript());

        final String standardScript = GroovyTranslator.of("g").translate(g.addV("customer")
                .property("customer_id", 501L)
                .property("name", "Foo\u0020Bar")
                .property("age", 25)
                .property("special", "`~!@#$%^&*()-_=+[{]}\\|;:'\",<.>/?")
                .asAdmin().getBytecode());

        final Bindings bindings = new SimpleBindings();
        bindings.putAll(parameterizedScript.getParameters());
        bindings.put("g", g);
        assertParameterizedScriptOk(standardScript, parameterizedScript.getScript(), bindings);
    }

    @Test
    public void shouldHandleVertexAndEdge() {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();

        final Object id1 = "customer:10:foo\u0020bar\u0020\u0024100#90"; // customer:10:foo bar $100#90
        final Vertex vertex1 = DetachedVertex.build().setLabel("customer").setId(id1)
                .create();
        final ParameterizedGroovyResult parameterizedScript1 = ParameterizedGroovyTranslator.of("g").parameterizedTranslate(g.inject(vertex1).asAdmin().getBytecode());
        assertEquals(2, parameterizedScript1.getParameters().size());
        assertEquals(id1, parameterizedScript1.getParameters().get("_args_0"));
        assertEquals("customer", parameterizedScript1.getParameters().get("_args_1"));
        assertEquals("g.inject(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(_args_0,_args_1, Collections.emptyMap()))", parameterizedScript1.getScript());
        final String standardScript1 = GroovyTranslator.of("g").translate(g.inject(vertex1).asAdmin().getBytecode());
        final Bindings bindings = new SimpleBindings();
        bindings.putAll(parameterizedScript1.getParameters());
        bindings.put("g", g);
        assertParameterizedScriptOk(standardScript1, parameterizedScript1.getScript(), bindings);


        final Object id2 = "user:20:foo\\u0020bar\\u005c\\u0022mr\\u005c\\u0022\\u00241000#50"; // user:20:foo\u0020bar\u005c\u0022mr\u005c\u0022\u00241000#50
        final Vertex vertex2 = DetachedVertex.build().setLabel("user").setId(id2)
                .create();
        final ParameterizedGroovyResult parameterizedScript2 = ParameterizedGroovyTranslator.of("g").parameterizedTranslate(g.inject(vertex2).asAdmin().getBytecode());
        assertEquals(2, parameterizedScript2.getParameters().size());
        assertEquals(id2, parameterizedScript2.getParameters().get("_args_0"));
        assertEquals("user", parameterizedScript2.getParameters().get("_args_1"));
        assertEquals("g.inject(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(_args_0,_args_1, Collections.emptyMap()))", parameterizedScript1.getScript());
        final String standardScript2 = GroovyTranslator.of("g").translate(g.inject(vertex2).asAdmin().getBytecode());
        bindings.putAll(parameterizedScript2.getParameters());
        bindings.put("g", g);
        assertParameterizedScriptOk(standardScript2, parameterizedScript2.getScript(), bindings);


        final Object id3 = "knows:30:foo\u0020bar\u0020\u0024100:\\u0020\\u0024500#70";
        final Edge edge = DetachedEdge.build().setLabel("knows").setId(id3)
                .setOutV((DetachedVertex) vertex1)
                .setInV((DetachedVertex) vertex2)
                .create();
        final ParameterizedGroovyResult parameterizedScript3 = ParameterizedGroovyTranslator.of("g").parameterizedTranslate(g.inject(edge).asAdmin().getBytecode());
        assertEquals(6, parameterizedScript3.getParameters().size());
        assertEquals(id3, parameterizedScript3.getParameters().get("_args_0"));
        assertEquals("knows", parameterizedScript3.getParameters().get("_args_1"));
        assertEquals(id1, parameterizedScript3.getParameters().get("_args_2"));
        assertEquals("customer", parameterizedScript3.getParameters().get("_args_3"));
        assertEquals(id2, parameterizedScript3.getParameters().get("_args_4"));
        assertEquals("user", parameterizedScript3.getParameters().get("_args_5"));
        assertEquals("g.inject(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge(_args_0,_args_1,Collections.emptyMap(),_args_2,_args_3,_args_4,_args_5))",parameterizedScript3.getScript());
        final String standardScript3 = GroovyTranslator.of("g").translate(g.inject(edge).asAdmin().getBytecode());
        bindings.putAll(parameterizedScript3.getParameters());
        bindings.put("g", g);
        assertParameterizedScriptOk(standardScript3, parameterizedScript3.getScript(), bindings);

        final ParameterizedGroovyResult parameterizedScript4 = ParameterizedGroovyTranslator.of("g").parameterizedTranslate(
                g.addE("knows").from(vertex1).to(vertex2).property("when", "2018/09/21")
                        .asAdmin().getBytecode());
        assertEquals(7, parameterizedScript4.getParameters().size());
        assertEquals("knows", parameterizedScript4.getParameters().get("_args_0"));
        assertEquals(id1, parameterizedScript4.getParameters().get("_args_1"));
        assertEquals("customer", parameterizedScript4.getParameters().get("_args_2"));
        assertEquals(id2, parameterizedScript4.getParameters().get("_args_3"));
        assertEquals("user", parameterizedScript4.getParameters().get("_args_4"));
        assertEquals("when", parameterizedScript4.getParameters().get("_args_5"));
        assertEquals("2018/09/21", parameterizedScript4.getParameters().get("_args_6"));
        assertEquals("g.addE(_args_0).from(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(_args_1,_args_2, Collections.emptyMap())).to(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(_args_3,_args_4, Collections.emptyMap())).property(_args_5,_args_6)",parameterizedScript4.getScript());
        final String standardScript4 = GroovyTranslator.of("g").translate(
                g.addE("knows").from(vertex1).to(vertex2).property("when", "2018/09/21")
                        .asAdmin().getBytecode());
        bindings.putAll(parameterizedScript4.getParameters());
        bindings.put("g", g);
        assertParameterizedScriptOk(standardScript4, parameterizedScript4.getScript(), bindings);
    }

    public static Object eval(final String s, final Object... args) throws ScriptException {
        return engine.eval(s, new SimpleBindings(ElementHelper.asMap(args)));
    }

    public static Object eval(final String s, final Bindings b) throws ScriptException {
        return engine.eval(s, b);
    }

    private void assertParameterizedTranslation(final String expectedTranslation, final Object... objs) {
        final ParameterizedGroovyResult parameterizedScript = ParameterizedGroovyTranslator.of("g").parameterizedTranslate(g.inject(objs).asAdmin().getBytecode());

        assertEquals(1, parameterizedScript.getParameters().size());
        assertEquals(objs[0], parameterizedScript.getParameters().get("_args_0"));
        assertEquals(String.format("g.inject(_args_0)", expectedTranslation), parameterizedScript.getScript());

        final String standardScript = GroovyTranslator.of("g").translate(g.inject(objs).asAdmin().getBytecode());
        final Bindings bindings = new SimpleBindings();
        bindings.putAll(parameterizedScript.getParameters());
        bindings.put("g", g);
        assertParameterizedScriptOk(parameterizedScript.getScript(), standardScript, bindings);
    }

    private void assertNonParameterizedTranslation(final String expectedTranslation, final Object... objs) {
        final ParameterizedGroovyResult parameterizedScript = ParameterizedGroovyTranslator.of("g").parameterizedTranslate(g.inject(objs).asAdmin().getBytecode());
        assertEquals(0, parameterizedScript.getParameters().size());
        assertEquals(String.format("g.inject(%s)", expectedTranslation),  parameterizedScript.getScript());

        final String standardScript = GroovyTranslator.of("g").translate(g.inject(objs).asAdmin().getBytecode());
        assertEquals(standardScript, parameterizedScript.getScript());
    }

    private void assertThatScriptOk(final String s, final Object... args) {
        try {
            assertNotNull(eval(s, args));
        } catch (ScriptException se) {
            se.printStackTrace();
            fail("Script should have eval'd");
        }
    }

    private void assertThatScriptOk(final String s, Bindings bindings) {
        try {
            assertNotNull(eval(s, bindings));
        } catch (ScriptException se) {
            se.printStackTrace();
            fail("Script should have eval'd");
        }
    }

    private void assertParameterizedScriptOk(final String standardScript, final String checkingScript, final Bindings bindings) {
        try {
            assertThatScriptOk(checkingScript, bindings);
            assertEquals(eval(standardScript, bindings), eval(checkingScript, bindings));
        } catch (ScriptException se) {
            se.printStackTrace();
            fail("Script should have eval'd");
        }
    }

    public static class ParameterizedSillyClass {

        private final String x;
        private final int y;

        private ParameterizedSillyClass(final String x, final int y) {
            this.x = x;
            this.y = y;
        }

        public static ParameterizedSillyClass from(final String x, final int y) {
            return new ParameterizedSillyClass(x, y);
        }

        public String getX() {
            return x;
        }

        public int getY() {
            return y;
        }

        @Override
        public String toString() {
            return x + ":" + String.valueOf(y);
        }
    }

    public static class ParameterizedSillyClassTranslator extends GroovyTranslator.DefaultTypeTranslator {

        @Override
        protected String convertToString(final Object object) {
            if (object instanceof ParameterizedSillyClass)
                return String.format("org.apache.tinkerpop.gremlin.groovy.jsr223.ParameterizedGroovyTranslatorTest.ParameterizedSillyClass.from('%s', (int) %s)",
                        ((ParameterizedSillyClass) object).getX(), ((ParameterizedSillyClass) object).getY());
            else
                return super.convertToString(object);
        }
    }

    public static class ParameterizedSillyClassTranslatorCustomizer implements TranslatorCustomizer {

        @Override
        public Translator.ScriptTranslator.TypeTranslator createTypeTranslator() {
            return new ParameterizedSillyClassTranslator();
        }
    }
}
