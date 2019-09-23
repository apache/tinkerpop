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

import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.jsr223.TranslatorCustomizer;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GroovyTranslatorTest {

    private Graph graph = TinkerGraph.open();
    private GraphTraversalSource g = graph.traversal();
    private static final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

    @Test
    public void shouldHandleStrategies() throws Exception {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal().withStrategies(SubgraphStrategy.create(new MapConfiguration(new HashMap<String, Object>() {{
            put(SubgraphStrategy.VERTICES, __.has("name", "marko"));
        }})));
        final Bindings bindings = new SimpleBindings();
        bindings.put("g", g);
        Traversal.Admin<Vertex, Object> traversal = engine.eval(g.V().values("name").asAdmin().getBytecode(), bindings, "g");
        assertEquals("marko", traversal.next());
        assertFalse(traversal.hasNext());
        //
        traversal = engine.eval(g.withoutStrategies(SubgraphStrategy.class).V().count().asAdmin().getBytecode(), bindings, "g");
        assertEquals(new Long(6), traversal.next());
        assertFalse(traversal.hasNext());
        //
        traversal = engine.eval(g.withStrategies(SubgraphStrategy.create(new MapConfiguration(new HashMap<String, Object>() {{
            put(SubgraphStrategy.VERTICES, __.has("name", "marko"));
        }})), ReadOnlyStrategy.instance()).V().values("name").asAdmin().getBytecode(), bindings, "g");
        assertEquals("marko", traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    public void shouldHandleConfusingSacks() {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();

        final Traversal<Vertex,Double> tConstantUnary = g.withSack(1.0, Lambda.unaryOperator("it + 1")).V().sack();
        final String scriptConstantUnary = GroovyTranslator.of("g").translate(tConstantUnary.asAdmin().getBytecode());
        assertEquals("g.withSack(1.0d, (java.util.function.UnaryOperator) {it + 1}).V().sack()", scriptConstantUnary);
        assertThatScriptOk(scriptConstantUnary, "g", g);

        final Traversal<Vertex,Double> tSupplierUnary = g.withSack(Lambda.supplier("1.0d"), Lambda.<Double>unaryOperator("it + 1")).V().sack();
        final String scriptSupplierUnary = GroovyTranslator.of("g").translate(tSupplierUnary.asAdmin().getBytecode());
        assertEquals("g.withSack((java.util.function.Supplier) {1.0d}, (java.util.function.UnaryOperator) {it + 1}).V().sack()", scriptSupplierUnary);
        assertThatScriptOk(scriptSupplierUnary, "g", g);

        final Traversal<Vertex,Double> tConstantBinary = g.withSack(1.0, Lambda.binaryOperator("x,y -> x + y + 1")).V().sack();
        final String scriptConstantBinary = GroovyTranslator.of("g").translate(tConstantBinary.asAdmin().getBytecode());
        assertEquals("g.withSack(1.0d, (java.util.function.BinaryOperator) {x,y -> x + y + 1}).V().sack()", scriptConstantBinary);
        assertThatScriptOk(scriptConstantBinary, "g", g);

        final Traversal<Vertex,Double> tSupplierBinary = g.withSack(Lambda.supplier("1.0d"), Lambda.<Double>binaryOperator("x,y -> x + y + 1")).V().sack();
        final String scriptSupplierBinary = GroovyTranslator.of("g").translate(tSupplierBinary.asAdmin().getBytecode());
        assertEquals("g.withSack((java.util.function.Supplier) {1.0d}, (java.util.function.BinaryOperator) {x,y -> x + y + 1}).V().sack()", scriptSupplierBinary);
        assertThatScriptOk(scriptSupplierBinary, "g", g);
    }

    @Test
    public void shouldSupportStringSupplierLambdas() {
        final TinkerGraph graph = TinkerFactory.createModern();
        GraphTraversalSource g = graph.traversal();
        g = g.withStrategies(new TranslationStrategy(g, GroovyTranslator.of("g"), false));
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

        final String script = GroovyTranslator.of("g").translate(t.getBytecode());
        assertEquals("g.withStrategies(org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategy.instance())" +
                        ".withSideEffect(\"lengthSum\",(int) 0).withSack((int) 1)" +
                        ".V()" +
                        ".filter({it.get().label().equals('person')})" +
                        ".flatMap({it.get().vertices(Direction.OUT)})" +
                        ".map({it.get().value('name').length()})" +
                        ".sideEffect({ x -> x.sideEffects(\"lengthSum\", x.<Integer>sideEffects('lengthSum') + x.get()) })" +
                        ".order().by({a,b -> a <=> b})" +
                        ".sack({ a,b -> a + b })",
                script);
    }

    @Test
    public void shouldHandleMaps() {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final String script = GroovyTranslator.of("g").translate(g.V().id().is(new LinkedHashMap<Object,Object>() {{
            put(3, "32");
            put(Arrays.asList(1, 2, 3.1d), 4);
        }}).asAdmin().getBytecode());
        assertEquals("g.V().id().is([((int) 3):(\"32\"),([(int) 1, (int) 2, 3.1d]):((int) 4)])", script);
        assertThatScriptOk(script, "g", g);
    }

    @Test
    public void shouldHandleEmptyMaps() {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final Function identity = new Lambda.OneArgLambda("it.get()", "gremlin-groovy");
        final String script = GroovyTranslator.of("g").translate(g.inject(Collections.emptyMap()).map(identity).asAdmin().getBytecode());
        assertEquals("g.inject([]).map({it.get()})", script);
        assertThatScriptOk(script, "g", g);
    }

    @Test
    public void shouldHandleDate() {
        final Calendar c = Calendar.getInstance();
        c.set(1975, Calendar.SEPTEMBER, 7);
        final Date d = c.getTime();
        assertTranslation(String.format("new java.util.Date(%s)", d.getTime()), d);
    }

    @Test
    public void shouldHandleTimestamp() {
        final Calendar c = Calendar.getInstance();
        c.set(1975, Calendar.SEPTEMBER, 7);
        final Timestamp t = new Timestamp(c.getTime().getTime());
        assertTranslation(String.format("new java.sql.Timestamp(%s)", t.getTime()), t);
    }

    @Test
    public void shouldHandleUuid() {
        final UUID uuid = UUID.fromString("ffffffff-fd49-1e4b-0000-00000d4b8a1d");
        assertTranslation(String.format("java.util.UUID.fromString('%s')", uuid), uuid);
    }

    @Test
    public void shouldHandleColumn() {
        assertTranslation("Column.keys", Column.keys);
    }

    @Test
    public void shouldHandleDirection() {
        assertTranslation("Direction.BOTH", Direction.BOTH);
    }

    @Test
    public void shouldHandleOrder() {
        assertTranslation("Order.decr", Order.decr);
    }

    @Test
    public void shouldHandlePop() {
        assertTranslation("Pop.last", Pop.last);
    }

    @Test
    public void shouldHandleScope() {
        assertTranslation("Scope.local", Scope.local);
    }

    @Test
    public void shouldIncludeCustomTypeTranslationForSomethingSilly() throws Exception {
        final TinkerGraph graph = TinkerGraph.open();
        final SillyClass notSillyEnough = SillyClass.from("not silly enough", 100);
        final GraphTraversalSource g = graph.traversal();

        // without type translation we get uglinesss
        final String scriptBad = GroovyTranslator.of("g").
                translate(g.inject(notSillyEnough).asAdmin().getBytecode());
        assertEquals(String.format("g.inject(%s)", "not silly enough:100"), scriptBad);

        // with type translation we get valid gremlin
        final String scriptGood = GroovyTranslator.of("g", new SillyClassTranslator()).
                translate(g.inject(notSillyEnough).asAdmin().getBytecode());
        assertEquals(String.format("g.inject(org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslatorTest.SillyClass.from('%s', (int) %s))", notSillyEnough.getX(), notSillyEnough.getY()), scriptGood);
        assertThatScriptOk(scriptGood, "g", g);

        final GremlinGroovyScriptEngine customEngine = new GremlinGroovyScriptEngine(new SillyClassTranslatorCustomizer());
        final Bindings b = new SimpleBindings();
        b.put("g", g);
        final Traversal t = customEngine.eval(g.inject(notSillyEnough).asAdmin().getBytecode(), b, "g");
        final SillyClass sc = (SillyClass) t.next();
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
        final String script = GroovyTranslator.of("g").translate(g.addV("customer")
                .property("customer_id", 501L)
                .property("name", "Foo\u0020Bar")
                .property("age", 25)
                .property("special", "`~!@#$%^&*()-_=+[{]}\\|;:'\",<.>/?")
                .asAdmin().getBytecode());

        assertEquals("g.addV(\"customer\")" +
                        ".property(\"customer_id\",501L)" +
                        ".property(\"name\",\"Foo Bar\")" +
                        ".property(\"age\",(int) 25)" +
                        ".property(\"special\",\"\"\"`~!@#\\$%^&*()-_=+[{]}\\\\|;:'\\\",<.>/?\"\"\")",
                script);
    }

    @Test
    public void shouldHandleVertexAndEdge() {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();

        final Object id1 = "customer:10:foo\u0020bar\u0020\u0024100#90"; // customer:10:foo bar $100#90
        final Vertex vertex1 = DetachedVertex.build().setLabel("customer").setId(id1)
                .create();
        final String script1 = GroovyTranslator.of("g").translate(g.inject(vertex1).asAdmin().getBytecode());
        assertEquals("g.inject(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(" +
                        "\"customer:10:foo bar \\$100#90\"," +
                        "\"customer\", Collections.emptyMap()))",
                script1);
        assertThatScriptOk(script1, "g", g);

        final Object id2 = "user:20:foo\\u0020bar\\u005c\\u0022mr\\u005c\\u0022\\u00241000#50"; // user:20:foo\u0020bar\u005c\u0022mr\u005c\u0022\u00241000#50
        final Vertex vertex2 = DetachedVertex.build().setLabel("user").setId(id2)
                .create();
        final String script2 = GroovyTranslator.of("g").translate(g.inject(vertex2).asAdmin().getBytecode());
        assertEquals("g.inject(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(" +
                        "\"user:20:foo\\\\u0020bar\\\\u005c\\\\u0022mr\\\\u005c\\\\u0022\\\\u00241000#50\"," +
                        "\"user\", Collections.emptyMap()))",
                script2);
        assertThatScriptOk(script2, "g", g);

        final Object id3 = "knows:30:foo\u0020bar\u0020\u0024100:\\u0020\\u0024500#70";
        final Edge edge = DetachedEdge.build().setLabel("knows").setId(id3)
                .setOutV((DetachedVertex) vertex1)
                .setInV((DetachedVertex) vertex2)
                .create();
        final String script3 = GroovyTranslator.of("g").translate(g.inject(edge).asAdmin().getBytecode());
        assertEquals("g.inject(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge(" +
                        "\"knows:30:foo bar \\$100:\\\\u0020\\\\u0024500#70\"," +
                        "\"knows\",Collections.emptyMap()," +
                        "\"customer:10:foo bar \\$100#90\",\"customer\"," +
                        "\"user:20:foo\\\\u0020bar\\\\u005c\\\\u0022mr\\\\u005c\\\\u0022\\\\u00241000#50\",\"user\"))",
                script3);
        assertThatScriptOk(script3, "g", g);

        final String script4 = GroovyTranslator.of("g").translate(
                g.addE("knows").from(vertex1).to(vertex2).property("when", "2018/09/21")
                        .asAdmin().getBytecode());
        assertEquals("g.addE(\"knows\")" +
                        ".from(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(\"customer:10:foo bar \\$100#90\",\"customer\", Collections.emptyMap()))" +
                        ".to(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(\"user:20:foo\\\\u0020bar\\\\u005c\\\\u0022mr\\\\u005c\\\\u0022\\\\u00241000#50\",\"user\", Collections.emptyMap()))" +
                        ".property(\"when\",\"2018/09/21\")",
                script4);
    }

    public static Object eval(final String s, final Object... args) throws ScriptException {
        return engine.eval(s, new SimpleBindings(ElementHelper.asMap(args)));
    }

    public static Object eval(final String s, final Bindings b) throws ScriptException {
        return engine.eval(s, b);
    }

    private void assertTranslation(final String expectedTranslation, final Object... objs) {
        final String script = GroovyTranslator.of("g").translate(g.inject(objs).asAdmin().getBytecode());
        assertEquals(String.format("g.inject(%s)", expectedTranslation), script);
        assertThatScriptOk(script, "g", g);
    }

    private void assertThatScriptOk(final String s, final Object... args) {
        try {
            assertNotNull(eval(s, args));
        } catch (ScriptException se) {
            se.printStackTrace();
            fail("Script should have eval'd");
        }
    }

    public static class SillyClass {

        private final String x;
        private final int y;

        private SillyClass(final String x, final int y) {
            this.x = x;
            this.y = y;
        }

        public static SillyClass from(final String x, final int y) {
            return new SillyClass(x, y);
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

    public static class SillyClassTranslator extends GroovyTranslator.DefaultTypeTranslator {

        @Override
        protected String convertToString(final Object object) {
            if (object instanceof SillyClass)
                return String.format("org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslatorTest.SillyClass.from('%s', (int) %s)",
                        ((SillyClass) object).getX(), ((SillyClass) object).getY());
            else
                return super.convertToString(object);
        }
    }

    public static class SillyClassTranslatorCustomizer implements TranslatorCustomizer {

        @Override
        public Translator.ScriptTranslator.TypeTranslator createTypeTranslator() {
            return new SillyClassTranslator();
        }
    }
}
