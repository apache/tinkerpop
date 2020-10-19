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
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Script;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.junit.Assert.assertEquals;

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

    private static final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());
    private static final Translator.ScriptTranslator translator = GroovyTranslator.of("g", true);

    @Test
    public void shouldHandleStrategies() throws Exception {
        assertEquals("g.withStrategies(org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy.instance(),org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy.create(new org.apache.commons.configuration2.MapConfiguration([(_args_0):(_args_1),(_args_2):(__.hasLabel(_args_3))]))).V().has(_args_4)",
                translator.translate(g.withStrategies(ReadOnlyStrategy.instance(),
                        SubgraphStrategy.build().checkAdjacentVertices(false).vertices(hasLabel("person")).create()).
                        V().has("name").asAdmin().getBytecode()).getScript());
    }

    @Test
    public void shouldSupportStringSupplierLambdas() {
        final GraphTraversal.Admin<Vertex, Integer> t = g.withSideEffect("lengthSum", 0).withSack(1)
                .V()
                .filter(Lambda.predicate("it.get().label().equals('person')"))
                .flatMap(Lambda.function("it.get().vertices(Direction.OUT)"))
                .map(Lambda.<Traverser<Object>, Integer>function("it.get().value('name').length()"))
                .sideEffect(Lambda.consumer("{ x -> x.sideEffects(\"lengthSum\", x.<Integer>sideEffects('lengthSum') + x.get()) }"))
                .order().by(Lambda.comparator("a,b -> a <=> b"))
                .sack(Lambda.biFunction("{ a,b -> a + b }"))
                .asAdmin();
        final Script script = translator.translate(t.getBytecode());
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
        assertEquals("g.withSideEffect(_args_0,_args_1).withSack(_args_2)" +
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
        final Script script = translator.translate(g.V().has(T.id, P.within(new ArrayList() {{
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
    }

    @Test
    public void shouldHandleSet() {
        final Script script = translator.translate(g.V().id().is(new HashSet<Object>() {{
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
    }

    @Test
    public void shouldHandleMaps() {
        final Script script = translator.translate(g.V().id().is(new LinkedHashMap<Object,Object>() {{
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
    }

    @Test
    public void shouldHandleEmptyMaps() {
        final Function identity = new Lambda.OneArgLambda("it.get()", "gremlin-groovy");
        final Script script = translator.translate(g.inject(Collections.emptyMap()).map(identity).asAdmin().getBytecode());
        final Bindings bindings = new SimpleBindings();
        script.getParameters().ifPresent(bindings::putAll);
        assertEquals(1, bindings.size());
        assertEquals(identity, bindings.get("_args_0"));
        assertEquals("g.inject([]).map(_args_0)", script.getScript());
    }

    @Test
    public void shouldIncludeCustomTypeTranslationForSomethingSilly() throws Exception {
        final ParameterizedSillyClass notSillyEnough = ParameterizedSillyClass.from("not silly enough", 100);

        // without type translation we get uglinesss
        final Script parameterizedScriptBad = translator.translate(g.inject(notSillyEnough).asAdmin().getBytecode());
        final Bindings bindings = new SimpleBindings();
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
        assertEquals("g.inject(org.apache.tinkerpop.gremlin.process.traversal.translator.ParameterizedGroovyTranslatorTest.ParameterizedSillyClass.from(_args_0,_args_1))",
                parameterizedScriptGood.getScript());
    }

    @Test
    public void shouldHaveValidToString() {
        assertEquals("translator[h:gremlin-groovy]", GroovyTranslator.of("h", true).toString());
    }

    @Test
    public void shouldEscapeStrings() {
        final Script script = translator.translate(g.addV("customer")
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
    }

    @Test
    public void shouldHandleVertexAndEdge() {
        final Object id1 = "customer:10:foo\u0020bar\u0020\u0024100#90"; // customer:10:foo bar $100#90
        final Vertex vertex1 = DetachedVertex.build().setLabel("customer").setId(id1)
                .create();
        final Script script1 = translator.translate(g.inject(vertex1).asAdmin().getBytecode());
        final Bindings bindings = new SimpleBindings();
        script1.getParameters().ifPresent(bindings::putAll);
        assertEquals(2, bindings.size());
        assertEquals(id1, bindings.get("_args_0"));
        assertEquals("customer", bindings.get("_args_1"));
        assertEquals("g.inject(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(_args_0,_args_1, Collections.emptyMap()))", script1.getScript());
        bindings.clear();

        final Object id2 = "user:20:foo\\u0020bar\\u005c\\u0022mr\\u005c\\u0022\\u00241000#50"; // user:20:foo\u0020bar\u005c\u0022mr\u005c\u0022\u00241000#50
        final Vertex vertex2 = DetachedVertex.build().setLabel("user").setId(id2)
                .create();
        final Script script2 = translator.translate(g.inject(vertex2).asAdmin().getBytecode());
        script2.getParameters().ifPresent(bindings::putAll);
        assertEquals(2, bindings.size());
        assertEquals(id2, bindings.get("_args_0"));
        assertEquals("user", bindings.get("_args_1"));
        assertEquals("g.inject(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex(_args_0,_args_1, Collections.emptyMap()))", script2.getScript());
        bindings.clear();

        final Object id3 = "knows:30:foo\u0020bar\u0020\u0024100:\\u0020\\u0024500#70";
        final Edge edge = DetachedEdge.build().setLabel("knows").setId(id3)
                .setOutV((DetachedVertex) vertex1)
                .setInV((DetachedVertex) vertex2)
                .create();
        final Script script3 = translator.translate(g.inject(edge).asAdmin().getBytecode());
        script3.getParameters().ifPresent(bindings::putAll);
        assertEquals(6, bindings.size());
        assertEquals(id3, bindings.get("_args_0"));
        assertEquals("knows", bindings.get("_args_1"));
        assertEquals(id1, bindings.get("_args_2"));
        assertEquals("customer", bindings.get("_args_3"));
        assertEquals(id2, bindings.get("_args_4"));
        assertEquals("user", bindings.get("_args_5"));
        assertEquals("g.inject(new org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge(_args_0,_args_1,Collections.emptyMap(),_args_2,_args_3,_args_4,_args_5))", script3.getScript());
        bindings.clear();

        final Script script4 = translator.translate(
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
        bindings.clear();

        final Script script5 = translator.translate(g.V().has("age").asAdmin().getBytecode());
        script5.getParameters().ifPresent(bindings::putAll);
        assertEquals(1, bindings.size());
        assertEquals("age", bindings.get("_args_0"));
        assertEquals("g.V().has(_args_0)", script5.getScript());
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

