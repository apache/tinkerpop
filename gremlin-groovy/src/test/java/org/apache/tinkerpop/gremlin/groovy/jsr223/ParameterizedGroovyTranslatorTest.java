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

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.jsr223.TranslatorCustomizer;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Script;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *  test {@link GroovyTranslator} which return parameterized result, covers:
 *   - parameterized script checking
 *   - binding checking
 *   - eval result checking
 *
 *  <p>
 *  {@link GroovyTranslatorTest } is used to test {@link GroovyTranslator}, both test cases looks the same
 *  <p>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Stark Arya (sandszhou.zj@alibaba-inc.com)
 */
public class ParameterizedGroovyTranslatorTest {

    private Graph graph = TinkerGraph.open();
    private GraphTraversalSource g = graph.traversal();
    private static final GremlinGroovyScriptEngine parameterizedEngine = new GremlinGroovyScriptEngine(new TranslatorCustomizer() {
        @Override
        public Translator.ScriptTranslator.TypeTranslator createTypeTranslator() {
            return new GroovyTranslator.DefaultTypeTranslator(true);
        }
    });

    @Test
    public void shouldHandleStrategies() throws Exception {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal().withStrategies(SubgraphStrategy.create(new MapConfiguration(new HashMap<String, Object>() {{
            put(SubgraphStrategy.VERTICES, __.has("name", "marko"));
        }})));
        final Bindings bindings = new SimpleBindings();
        bindings.put("g", g);
        Traversal.Admin<Vertex, Object> traversal = parameterizedEngine.eval(g.V().values("name").asAdmin().getBytecode(), bindings, "g");
        assertEquals("marko", traversal.next());
        assertFalse(traversal.hasNext());
        //
        traversal = parameterizedEngine.eval(g.withoutStrategies(SubgraphStrategy.class).V().count().asAdmin().getBytecode(), bindings, "g");
        assertEquals(new Long(6), traversal.next());
        assertFalse(traversal.hasNext());
        //
        traversal = parameterizedEngine.eval(g.withStrategies(SubgraphStrategy.create(new MapConfiguration(new HashMap<String, Object>() {{
            put(SubgraphStrategy.VERTICES, __.has("name", "marko"));
        }})), ReadOnlyStrategy.instance()).V().values("name").asAdmin().getBytecode(), bindings, "g");
        assertEquals("marko", traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    public void shouldSupportStringSupplierLambdas() {
        final TinkerGraph graph = TinkerFactory.createModern();
        GraphTraversalSource g = graph.traversal();
        g = g.withStrategies(new TranslationStrategy(g, GroovyTranslator.of("g", true), false));
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

        final Script script = GroovyTranslator.of("g", true).translate(t.getBytecode());
        final Bindings bindings = new SimpleBindings();
        script.getParameters().ifPresent(bindings::putAll);
        assertEquals(9, bindings.size());
        assertEquals("lengthSum", bindings.get("_args_0"));
        assertEquals(Integer.valueOf(0), bindings.get("_args_1"));
        assertEquals(Integer.valueOf(1), bindings.get("_args_2"));
        assertEquals(Lambda.predicate("it.get().label().equals('person')"), bindings.get("_args_3"));
        assertEquals(Lambda.function("it.get().vertices(Direction.OUT)"), bindings.get("_args_4"));
        assertEquals(Lambda.<Traverser<Object>, Integer>function("it.get().value('name').length()"), bindings.get("_args_5"));
        assertEquals(Lambda.consumer("{ x -> x.sideEffects(\"lengthSum\", x.<Integer>sideEffects('lengthSum') + x.get()) }"), bindings.get("_args_6"));
        assertEquals(Lambda.comparator("a,b -> a <=> b"), bindings.get("_args_7"));
        assertEquals(Lambda.biFunction("{ a,b -> a + b }"), bindings.get("_args_8"));
        assertEquals("g.withStrategies(org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategy.instance())" +
                        ".withSideEffect(_args_0,_args_1).withSack(_args_2)" +
                        ".V()" +
                        ".filter(_args_3)" +
                        ".flatMap(_args_4)" +
                        ".map(_args_5)" +
                        ".sideEffect(_args_6)" +
                        ".order().by(_args_7)" +
                        ".sack(_args_8)",
                script.getScript());
    }

    @Test
    public void shouldHandleArray() {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final Script script = GroovyTranslator.of("g", true).translate(g.V().has(T.id, P.within(new ArrayList() {{
            add(1);
            add(2);
            add(3);
            add(4);
            add(5);
        }})).asAdmin().getBytecode());
        final Bindings bindings = new SimpleBindings();
        script.getParameters().ifPresent(bindings::putAll);
        assertEquals(5, bindings.size());
        assertEquals(Integer.valueOf(1), bindings.get("_args_0"));
        assertEquals(Integer.valueOf(2), bindings.get("_args_1"));
        assertEquals(Integer.valueOf(3), bindings.get("_args_2"));
        assertEquals(Integer.valueOf(4), bindings.get("_args_3"));
        assertEquals(Integer.valueOf(5), bindings.get("_args_4"));
        assertEquals("g.V().has(T.id,P.within([_args_0, _args_1, _args_2, _args_3, _args_4]))", script.getScript());

        final Script standard = GroovyTranslator.of("g").translate(g.V().has(T.id, P.within(new ArrayList() {{
            add(1);
            add(2);
            add(3);
            add(4);
            add(5);
        }})).asAdmin().getBytecode());

        bindings.put("g", g);
        assertParameterizedScriptOk(standard.getScript(), script.getScript(), bindings, true);
    }

    @Test
    public void shouldHandleSet() {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final Script script = GroovyTranslator.of("g", true).translate(g.V().id().is(new HashSet<Object>() {{
            add(3);
            add(Arrays.asList(1, 2, 3.1d));
            add(3);
            add("3");
        }}).asAdmin().getBytecode());
        final Bindings bindings = new SimpleBindings();
        script.getParameters().ifPresent(bindings::putAll);
        assertEquals(5, bindings.size());
        assertEquals(Integer.valueOf(3), bindings.get("_args_0"));
        assertEquals("3", bindings.get("_args_1"));
        assertEquals(Integer.valueOf(1), bindings.get("_args_2"));
        assertEquals(Integer.valueOf(2), bindings.get("_args_3"));
        assertEquals(Double.valueOf(3.1), bindings.get("_args_4"));
        assertEquals("g.V().id().is([_args_0, _args_1, [_args_2, _args_3, _args_4]] as Set)", script.getScript());

        final Script standard = GroovyTranslator.of("g" ).translate(g.V().id().is(new HashSet<Object>() {{
            add(3);
            add(Arrays.asList(1, 2, 3.1d));
            add(3);
            add("3");
        }}).asAdmin().getBytecode());
        bindings.put("g", g);
        assertParameterizedScriptOk(standard.getScript(), script.getScript(), bindings, true);
    }

    @Test
    public void shouldHandleMaps() {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final Script script = GroovyTranslator.of("g", true).translate(g.V().id().is(new LinkedHashMap<Object,Object>() {{
            put(3, "32");
            put(Arrays.asList(1, 2, 3.1d), 4);
        }}).asAdmin().getBytecode());
        final Bindings bindings = new SimpleBindings();
        script.getParameters().ifPresent(bindings::putAll);
        assertEquals(6, bindings.size());
        assertEquals(Integer.valueOf(3), bindings.get("_args_0"));
        assertEquals("32", bindings.get("_args_1"));
        assertEquals(Integer.valueOf(1), bindings.get("_args_2"));
        assertEquals(Integer.valueOf(2), bindings.get("_args_3"));
        assertEquals(Double.valueOf(3.1), bindings.get("_args_4"));
        assertEquals(Integer.valueOf(4), bindings.get("_args_5"));
        assertEquals("g.V().id().is([(_args_0):(_args_1),([_args_2, _args_3, _args_4]):(_args_5)])", script.getScript());
        final Script standard = GroovyTranslator.of("g").translate(g.V().id().is(new LinkedHashMap<Object,Object>() {{
            put(3, "32");
            put(Arrays.asList(1, 2, 3.1d), 4);
        }}).asAdmin().getBytecode());
        bindings.put("g", g);
        assertParameterizedScriptOk(standard.getScript(), script.getScript(), bindings, true);
    }

    @Test
    public void shouldHandleEmptyMaps() {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final Function identity = new Lambda.OneArgLambda("it.get()", "gremlin-groovy");
        final Script script = GroovyTranslator.of("g", true).translate(g.inject(Collections.emptyMap()).map(identity).asAdmin().getBytecode());
        final Bindings bindings = new SimpleBindings();
        script.getParameters().ifPresent(bindings::putAll);
        assertEquals(1, bindings.size());
        assertEquals(identity, bindings.get("_args_0"));
        assertEquals("g.inject([]).map(_args_0)", script.getScript());
        bindings.put("g", g);
        assertThatScriptOk(script.getScript(), bindings);
    }

    @Test
    public void shouldHandleDate() {
        final Calendar c = Calendar.getInstance();
        c.set(1975, Calendar.SEPTEMBER, 7);
        final Date d = c.getTime();
        assertParameterizedTranslation(String.format("new java.util.Date(%s)", d.getTime()), d);
    }

    @Test
    public void shouldHandlePredicate() {
        final P p = new P(Compare.eq, 10);
        assertParameterizedTranslation(String.format("new java.util.Date(%s)", p.toString()), 10);
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
    public void shouldHandleClass() {
        final Class cls = Vertex.class;
        assertParameterizedTranslation(String.format("%s", cls), cls.getCanonicalName());
    }

    @Test
    public void shouldHandleBarrier() {
        final SackFunctions.Barrier barrier = SackFunctions.Barrier.normSack;
        assertNonParameterizedTranslation(String.format("SackFunctions.Barrier.%s", barrier), barrier);
    }

    @Test
    public void shouldHandleCardinality() {
        final VertexProperty.Cardinality cardinality = VertexProperty.Cardinality.set;
        assertNonParameterizedTranslation(String.format("VertexProperty.Cardinality.%s", cardinality), cardinality);
    }

    @Test
    public void shouldHandlePick() {
        final TraversalOptionParent.Pick pick = TraversalOptionParent.Pick.any;
        assertNonParameterizedTranslation(String.format("TraversalOptionParent.Pick.%s", pick), pick);
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
        assertNonParameterizedTranslation("Order.shuffle", Order.shuffle);
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
        final Script parameterizedScriptBad = GroovyTranslator.of("g", true).
                translate(g.inject(notSillyEnough).asAdmin().getBytecode());
        Bindings bindings = new SimpleBindings();
        parameterizedScriptBad.getParameters().ifPresent(bindings::putAll);
        assertEquals(String.format("g.inject(%s)", "_args_0"), parameterizedScriptBad.getScript());
        assertEquals(1, bindings.size());
        assertEquals(notSillyEnough, bindings.get("_args_0"));
        bindings.clear();

        // with type translation we get valid gremlin
        final Script parameterizedScriptGood = GroovyTranslator.of("g", new ParameterizedSillyClassTranslatorCustomizer().createTypeTranslator()).
                translate(g.inject(notSillyEnough).asAdmin().getBytecode());
        parameterizedScriptGood.getParameters().ifPresent(bindings::putAll);
        assertEquals(2, bindings.size());
        assertEquals(notSillyEnough.getX(), bindings.get("_args_0"));
        assertEquals(notSillyEnough.getY(), bindings.get("_args_1"));
        assertEquals("g.inject(org.apache.tinkerpop.gremlin.groovy.jsr223.ParameterizedGroovyTranslatorTest.ParameterizedSillyClass.from(_args_0,_args_1))", parameterizedScriptGood.getScript());
        bindings.put("g", g);
        assertThatScriptOk(parameterizedScriptGood.getScript(), bindings);

        final GremlinGroovyScriptEngine customEngine = new GremlinGroovyScriptEngine(new ParameterizedSillyClassTranslatorCustomizer());
        final Traversal t = customEngine.eval(g.inject(notSillyEnough).asAdmin().getBytecode(), bindings, "g");
        final ParameterizedSillyClass sc = (ParameterizedSillyClass) t.next();
        assertEquals(notSillyEnough.getX(), sc.getX());
        assertEquals(notSillyEnough.getY(), sc.getY());
        assertThat(t.hasNext(), is(false));
    }

    @Test
    public void shouldHaveValidToString() {
        assertEquals("translator[h:gremlin-groovy]", GroovyTranslator.of("h", true).toString());
    }

    @Test
    public void shouldEscapeStrings() {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final Script script = GroovyTranslator.of("g", true).translate(g.addV("customer")
                .property("customer_id", 501L)
                .property("name", "Foo\u0020Bar")
                .property("age", 25)
                .property("special", "`~!@#$%^&*()-_=+[{]}\\|;:'\",<.>/?")
                .asAdmin().getBytecode());
        final Bindings bindings = new SimpleBindings();
        script.getParameters().ifPresent(bindings::putAll);
        assertEquals(9, bindings.size());
        assertEquals("customer", bindings.get("_args_0"));
        assertEquals("customer_id", bindings.get("_args_1"));
        assertEquals(Long.valueOf(501), bindings.get("_args_2"));
        assertEquals("name", bindings.get("_args_3"));
        assertEquals("Foo\u0020Bar", bindings.get("_args_4"));
        assertEquals("age", bindings.get("_args_5"));
        assertEquals(Integer.valueOf(25), bindings.get("_args_6"));
        assertEquals("special", bindings.get("_args_7"));
        assertEquals("`~!@#$%^&*()-_=+[{]}\\|;:'\",<.>/?", bindings.get("_args_8"));
        assertEquals("g.addV(_args_0).property(_args_1,_args_2).property(_args_3,_args_4).property(_args_5,_args_6).property(_args_7,_args_8)", script.getScript());

        final Script standard = GroovyTranslator.of("g").translate(g.addV("customer")
                .property("customer_id", 501L)
                .property("name", "Foo\u0020Bar")
                .property("age", 25)
                .property("special", "`~!@#$%^&*()-_=+[{]}\\|;:'\",<.>/?")
                .asAdmin().getBytecode());

        bindings.put("g", g);
        //add vertex will return different vertex id
        assertParameterizedScriptOk(standard.getScript(), script.getScript(), bindings, false);
    }

    @Test
    public void shouldHandleVertexAndEdge() {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();

        final Object id1 = "customer:10:foo\u0020bar\u0020\u0024100#90"; // customer:10:foo bar $100#90
        final Vertex vertex1 = DetachedVertex.build().setLabel("customer").setId(id1)
                .create();
        final Script script1 = GroovyTranslator.of("g", true).translate(g.inject(vertex1).asAdmin().getBytecode());
        final Bindings bindings = new SimpleBindings();
        script1.getParameters().ifPresent(bindings::putAll);
        assertEquals(2, bindings.size());
        assertEquals(id1, bindings.get("_args_0"));
        assertEquals("customer", bindings.get("_args_1"));
        assertEquals("g.inject(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(_args_0,_args_1, Collections.emptyMap()))", script1.getScript());
        final Script standard1 = GroovyTranslator.of("g").translate(g.inject(vertex1).asAdmin().getBytecode());
        bindings.put("g", g);
        // TinkerGraph not support string id
        assertParameterizedScriptOk(standard1.getScript(), script1.getScript(), bindings, false);
        bindings.clear();

        final Object id2 = "user:20:foo\\u0020bar\\u005c\\u0022mr\\u005c\\u0022\\u00241000#50"; // user:20:foo\u0020bar\u005c\u0022mr\u005c\u0022\u00241000#50
        final Vertex vertex2 = DetachedVertex.build().setLabel("user").setId(id2)
                .create();
        final Script script2 = GroovyTranslator.of("g", true).translate(g.inject(vertex2).asAdmin().getBytecode());
        script2.getParameters().ifPresent(bindings::putAll);
        assertEquals(2, bindings.size());
        assertEquals(id2, bindings.get("_args_0"));
        assertEquals("user", bindings.get("_args_1"));
        assertEquals("g.inject(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(_args_0,_args_1, Collections.emptyMap()))", script2.getScript());
        final Script standard2 = GroovyTranslator.of("g").translate(g.inject(vertex2).asAdmin().getBytecode());
        bindings.put("g", g);
        assertParameterizedScriptOk(standard2.getScript(), script2.getScript(), bindings, false);
        bindings.clear();

        final Object id3 = "knows:30:foo\u0020bar\u0020\u0024100:\\u0020\\u0024500#70";
        final Edge edge = DetachedEdge.build().setLabel("knows").setId(id3)
                .setOutV((DetachedVertex) vertex1)
                .setInV((DetachedVertex) vertex2)
                .create();
        final Script script3 = GroovyTranslator.of("g", true).translate(g.inject(edge).asAdmin().getBytecode());
        script3.getParameters().ifPresent(bindings::putAll);
        assertEquals(6, bindings.size());
        assertEquals(id3, bindings.get("_args_0"));
        assertEquals("knows", bindings.get("_args_1"));
        assertEquals(id1, bindings.get("_args_2"));
        assertEquals("customer", bindings.get("_args_3"));
        assertEquals(id2, bindings.get("_args_4"));
        assertEquals("user", bindings.get("_args_5"));
        assertEquals("g.inject(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge(_args_0,_args_1,Collections.emptyMap(),_args_2,_args_3,_args_4,_args_5))", script3.getScript());
        final Script standard3 = GroovyTranslator.of("g").translate(g.inject(edge).asAdmin().getBytecode());
        bindings.put("g", g);
        assertParameterizedScriptOk(standard3.getScript(), script3.getScript(), bindings, false);
        bindings.clear();

        final Script script4 = GroovyTranslator.of("g", true).translate(
                g.addE("knows").from(vertex1).to(vertex2).property("when", "2018/09/21")
                        .asAdmin().getBytecode());
        script4.getParameters().ifPresent(bindings::putAll);
        assertEquals(7, bindings.size());
        assertEquals("knows", bindings.get("_args_0"));
        assertEquals(id1, bindings.get("_args_1"));
        assertEquals("customer", bindings.get("_args_2"));
        assertEquals(id2, bindings.get("_args_3"));
        assertEquals("user", bindings.get("_args_4"));
        assertEquals("when", bindings.get("_args_5"));
        assertEquals("2018/09/21", bindings.get("_args_6"));
        assertEquals("g.addE(_args_0).from(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(_args_1,_args_2, Collections.emptyMap())).to(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(_args_3,_args_4, Collections.emptyMap())).property(_args_5,_args_6)", script4.getScript());
        final Script standard4 = GroovyTranslator.of("g").translate(
                g.addE("knows").from(vertex1).to(vertex2).property("when", "2018/09/21")
                        .asAdmin().getBytecode());
        bindings.put("g", g);
        assertParameterizedScriptOk(standard4.getScript(), script4.getScript(), bindings, false);
        bindings.clear();

        final Script script5 = GroovyTranslator.of("g", true).translate(g.V().has("age").asAdmin().getBytecode());
        script5.getParameters().ifPresent(bindings::putAll);
        assertEquals(1, bindings.size());
        assertEquals("age", bindings.get("_args_0"));
        assertEquals("g.V().has(_args_0)", script5.getScript());
        final Script standard5 = GroovyTranslator.of("g").translate(g.V().has("age").asAdmin().getBytecode());
        bindings.put("g", g);
        // Ok, here we checking all the result
        assertParameterizedScriptOk(standard5.getScript(), script5.getScript(), bindings, true);
        bindings.clear();
    }

    private Object eval(final String s, final Bindings b) throws ScriptException {
        return parameterizedEngine.eval(s, b);
    }

    private void assertParameterizedTranslation(final String expectedTranslation, final Object... objs) {
        final Script script = GroovyTranslator.of("g", true).translate(g.inject(objs).asAdmin().getBytecode());
        final Bindings bindings = new SimpleBindings();
        script.getParameters().ifPresent(bindings::putAll);
        assertEquals(1, bindings.size());
        assertEquals(objs[0], bindings.get("_args_0"));
        assertEquals(String.format("g.inject(_args_0)", expectedTranslation), script.getScript());

        final Script standard = GroovyTranslator.of("g").translate(g.inject(objs).asAdmin().getBytecode());
        bindings.put("g", g);
        assertParameterizedScriptOk(standard.getScript(), script.getScript(), bindings, true);
    }

    private void assertNonParameterizedTranslation(final String expectedTranslation, final Object... objs) {
        final Script script = GroovyTranslator.of("g", true).translate(g.inject(objs).asAdmin().getBytecode());
        final Bindings bindings = new SimpleBindings();
        script.getParameters().ifPresent(bindings::putAll);
        assertEquals(0, bindings.size());
        assertEquals(String.format("g.inject(%s)", expectedTranslation),  script.getScript());

        final Script standard = GroovyTranslator.of("g").translate(g.inject(objs).asAdmin().getBytecode());
        assertEquals(standard.getScript(), script.getScript());
    }

    private void assertThatScriptOk(final String s, Bindings bindings) {
        try {
            assertNotNull(eval(s, bindings));
        } catch (ScriptException se) {
            se.printStackTrace();
            fail("Script should have eval'd");
        }
    }

    private void assertParameterizedScriptOk(final String standardScript, final String checkingScript, final Bindings bindings, final boolean resultChecking) {
        try {
            System.out.println(standardScript + " " + checkingScript);
            assertTrue(!standardScript.equals(checkingScript));
            assertThatScriptOk(checkingScript, bindings);
            assertEquals(eval(standardScript, bindings), eval(checkingScript, bindings));
            if (resultChecking) {
                Traversal.Admin<Vertex, Object> standardTraversal = (Traversal.Admin) eval(standardScript, bindings);
                Traversal.Admin<Vertex, Object> checkingTraversal = (Traversal.Admin) eval(checkingScript, bindings);
                while (standardTraversal.hasNext() && checkingTraversal.hasNext()) {
                    // assertEquals(standardTraversal.next(), checkingTraversal.next());
                    System.out.println(standardTraversal.next() +  " " + checkingTraversal.next());
                }
                assertEquals(standardTraversal.hasNext(), checkingTraversal.hasNext());
            }
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

        public Object[] getArguments() {
            return new Object[] {x,y};
        }

        @Override
        public String toString() {
            return String.format("org.apache.tinkerpop.gremlin.groovy.jsr223.ParameterizedGroovyTranslatorTest.ParameterizedSillyClass.from('%s', (int) %s)", getX(), getY());
        }
    }

    public static class ParameterizedSillyClassTranslator extends  GroovyTranslator.DefaultTypeTranslator {
        public ParameterizedSillyClassTranslator(final boolean withParameters) {
           super(withParameters);
        }

        @Override
        protected Script convertToScript(final Object object) {
            if (object instanceof ParameterizedSillyClass) {
                ParameterizedSillyClass obj = (ParameterizedSillyClass) object;
                script.append(obj.getClass().getCanonicalName());
                if (0 == obj.getArguments().length) {
                    script.append(".").append("from").append("()");
                } else {
                    script.append(".").append("from").append("(");
                    for (final Object argument: obj.getArguments()) {
                        convertToScript(argument);
                        script.append(",");
                    }
                    script.setCharAtEnd(')');
                }
                return script;
            } else {
                return super.convertToScript(object);
            }
        }
    }

    public static class ParameterizedSillyClassTranslatorCustomizer implements TranslatorCustomizer {

        @Override
        public Translator.ScriptTranslator.TypeTranslator createTypeTranslator() {
            return new ParameterizedSillyClassTranslator(true);
        }
    }
}

