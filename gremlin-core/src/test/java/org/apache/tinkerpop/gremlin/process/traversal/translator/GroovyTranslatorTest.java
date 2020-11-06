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

package org.apache.tinkerpop.gremlin.process.traversal.translator;

import org.apache.tinkerpop.gremlin.jsr223.TranslatorCustomizer;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GroovyTranslatorTest {
    private static final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());
    private static final Translator.ScriptTranslator translator = GroovyTranslator.of("g");

    @Test
    public void shouldTranslateStrategies() throws Exception {
        assertEquals("g.withStrategies(ReadOnlyStrategy,new SubgraphStrategy(checkAdjacentVertices: false, vertices: __.hasLabel(\"person\"))).V().has(\"name\")",
                translator.translate(g.withStrategies(ReadOnlyStrategy.instance(),
                        SubgraphStrategy.build().checkAdjacentVertices(false).vertices(hasLabel("person")).create()).
                        V().has("name")));
    }

    @Test
    public void shouldTranslateConfusingSacks() {
        final Traversal<Vertex,Double> tConstantUnary = g.withSack(1.0, Lambda.unaryOperator("it + 1")).V().sack();
        final String scriptConstantUnary = translator.translate(tConstantUnary);
        assertEquals("g.withSack(1.0d, (java.util.function.UnaryOperator) {it + 1}).V().sack()", scriptConstantUnary);

        final Traversal<Vertex,Double> tSupplierUnary = g.withSack(Lambda.supplier("1.0d"), Lambda.<Double>unaryOperator("it + 1")).V().sack();
        final String scriptSupplierUnary = translator.translate(tSupplierUnary);
        assertEquals("g.withSack((java.util.function.Supplier) {1.0d}, (java.util.function.UnaryOperator) {it + 1}).V().sack()", scriptSupplierUnary);

        final Traversal<Vertex,Double> tConstantBinary = g.withSack(1.0, Lambda.binaryOperator("x,y -> x + y + 1")).V().sack();
        final String scriptConstantBinary = translator.translate(tConstantBinary);
        assertEquals("g.withSack(1.0d, (java.util.function.BinaryOperator) {x,y -> x + y + 1}).V().sack()", scriptConstantBinary);

        final Traversal<Vertex,Double> tSupplierBinary = g.withSack(Lambda.supplier("1.0d"), Lambda.<Double>binaryOperator("x,y -> x + y + 1")).V().sack();
        final String scriptSupplierBinary = translator.translate(tSupplierBinary);
        assertEquals("g.withSack((java.util.function.Supplier) {1.0d}, (java.util.function.BinaryOperator) {x,y -> x + y + 1}).V().sack()", scriptSupplierBinary);
    }

    @Test
    public void shouldTranslateStringSupplierLambdas() {
        final GraphTraversal.Admin<Vertex, Integer> t = g.withSideEffect("lengthSum", 0).withSack(1)
                .V()
                .filter(Lambda.predicate("it.get().label().equals('person')"))
                .flatMap(Lambda.function("it.get().vertices(Direction.OUT)"))
                .map(Lambda.<Traverser<Object>, Integer>function("it.get().value('name').length()"))
                .sideEffect(Lambda.consumer("{ x -> x.sideEffects(\"lengthSum\", x.<Integer>sideEffects('lengthSum') + x.get()) }"))
                .order().by(Lambda.comparator("a,b -> a <=> b"))
                .sack(Lambda.biFunction("{ a,b -> a + b }"))
                .asAdmin();

        final String script = translator.translate(t.getBytecode());
        assertEquals(   "g.withSideEffect(\"lengthSum\",(int) 0).withSack((int) 1)" +
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
    public void shouldTranslateMaps() {
        final String script = translator.translate(g.V().id().is(new LinkedHashMap<Object,Object>() {{
            put(3, "32");
            put(Arrays.asList(1, 2, 3.1d), 4);
        }}));
        assertEquals("g.V().id().is([((int) 3):(\"32\"),([(int) 1, (int) 2, 3.1d]):((int) 4)])", script);
    }

    @Test
    public void shouldTranslateEmptyMaps() {
        final Function identity = new Lambda.OneArgLambda("it.get()", "gremlin-groovy");
        final String script = translator.translate(g.inject(Collections.emptyMap()).map(identity));
        assertEquals("g.inject([]).map({it.get()})", script);
    }

    @Test
    public void shouldTranslateDate() {
        final Calendar c = Calendar.getInstance();
        c.set(1975, Calendar.SEPTEMBER, 7);
        final Date d = c.getTime();
        assertTranslation(String.format("new Date(%s)", d.getTime()), d);
    }

    @Test
    public void shouldTranslateTimestamp() {
        final Calendar c = Calendar.getInstance();
        c.set(1975, Calendar.SEPTEMBER, 7);
        final Timestamp t = new Timestamp(c.getTime().getTime());
        assertTranslation(String.format("new Timestamp(%s)", t.getTime()), t);
    }

    @Test
    public void shouldTranslateUuid() {
        final UUID uuid = UUID.fromString("ffffffff-fd49-1e4b-0000-00000d4b8a1d");
        assertTranslation(String.format("UUID.fromString('%s')", uuid), uuid);
    }

    @Test
    public void shouldTranslateColumn() {
        assertTranslation("Column.keys", Column.keys);
    }

    @Test
    public void shouldTranslateDirection() {
        assertTranslation("Direction.BOTH", Direction.BOTH);
    }

    @Test
    public void shouldTranslateOrder() {
        assertTranslation("Order.decr", Order.decr);
    }

    @Test
    public void shouldTranslatePop() {
        assertTranslation("Pop.last", Pop.last);
    }

    @Test
    public void shouldTranslateScope() {
        assertTranslation("Scope.local", Scope.local);
    }

    @Test
    public void shouldIncludeCustomTypeTranslationForSomethingSilly() throws Exception {
        final SillyClass notSillyEnough = SillyClass.from("not silly enough", 100);

        // without type translation we get uglinesss
        final String scriptBad = translator.
                translate(g.inject(notSillyEnough));
        assertEquals(String.format("g.inject(%s)", "not silly enough:100"), scriptBad);

        // with type translation we get valid gremlin
        final String scriptGood = GroovyTranslator.of("g", new SillyClassTranslator()).
                translate(g.inject(notSillyEnough));
        assertEquals(String.format("g.inject(org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslatorTest.SillyClass.from('%s', (int) %s))", notSillyEnough.getX(), notSillyEnough.getY()), scriptGood);
    }

    @Test
    public void shouldHaveValidToString() {
        assertEquals("translator[h:gremlin-groovy]", GroovyTranslator.of("h").toString());
    }

    @Test
    public void shouldEscapeStrings() {
        final String script = translator.translate(g.addV("customer")
                .property("customer_id", 501L)
                .property("name", "Foo\u0020Bar")
                .property("age", 25)
                .property("special", "`~!@#$%^&*()-_=+[{]}\\|;:'\",<.>/?")
                );

        assertEquals("g.addV(\"customer\")" +
                        ".property(\"customer_id\",501L)" +
                        ".property(\"name\",\"Foo Bar\")" +
                        ".property(\"age\",(int) 25)" +
                        ".property(\"special\",\"\"\"`~!@#\\$%^&*()-_=+[{]}\\\\|;:'\\\",<.>/?\"\"\")",
                script);
    }

    @Test
    public void shouldTranslateVertexAndEdge() {
        final Object id1 = "customer:10:foo\u0020bar\u0020\u0024100#90"; // customer:10:foo bar $100#90
        final Vertex vertex1 = DetachedVertex.build().setLabel("customer").setId(id1)
                .create();
        final String script1 = translator.translate(g.inject(vertex1));
        assertEquals("g.inject(new ReferenceVertex(" +
                        "\"customer:10:foo bar \\$100#90\"," +
                        "\"customer\"))",
                script1);

        final Object id2 = "user:20:foo\\u0020bar\\u005c\\u0022mr\\u005c\\u0022\\u00241000#50"; // user:20:foo\u0020bar\u005c\u0022mr\u005c\u0022\u00241000#50
        final Vertex vertex2 = DetachedVertex.build().setLabel("user").setId(id2)
                .create();
        final String script2 = translator.translate(g.inject(vertex2));
        assertEquals("g.inject(new ReferenceVertex(" +
                        "\"user:20:foo\\\\u0020bar\\\\u005c\\\\u0022mr\\\\u005c\\\\u0022\\\\u00241000#50\"," +
                        "\"user\"))",
                script2);

        final Object id3 = "knows:30:foo\u0020bar\u0020\u0024100:\\u0020\\u0024500#70";
        final Edge edge = DetachedEdge.build().setLabel("knows").setId(id3)
                .setOutV((DetachedVertex) vertex1)
                .setInV((DetachedVertex) vertex2)
                .create();
        final String script3 = translator.translate(g.inject(edge));
        assertEquals("g.inject(new ReferenceEdge(" +
                        "\"knows:30:foo bar \\$100:\\\\u0020\\\\u0024500#70\"," +
                        "\"knows\"," +
                        "new ReferenceVertex(\"user:20:foo\\\\u0020bar\\\\u005c\\\\u0022mr\\\\u005c\\\\u0022\\\\u00241000#50\",\"user\")," +
                        "new ReferenceVertex(\"customer:10:foo bar \\$100#90\",\"customer\")))",
                script3);

        final String script4 = translator.translate(
                g.addE("knows").from(vertex1).to(vertex2).property("when", "2018/09/21")
                        );
        assertEquals("g.addE(\"knows\")" +
                        ".from(new ReferenceVertex(\"customer:10:foo bar \\$100#90\",\"customer\"))" +
                        ".to(new ReferenceVertex(\"user:20:foo\\\\u0020bar\\\\u005c\\\\u0022mr\\\\u005c\\\\u0022\\\\u00241000#50\",\"user\"))" +
                        ".property(\"when\",\"2018/09/21\")",
                script4);
    }

    private void assertTranslation(final String expectedTranslation, final Object... objs) {
        final String script = translator.translate(g.inject(objs));
        assertEquals(String.format("g.inject(%s)", expectedTranslation), script);
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
