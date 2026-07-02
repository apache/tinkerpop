/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefined;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PrimitivePDTAdapter;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PrimitivePDT;
import org.apache.tinkerpop.gremlin.structure.io.pdt.CompositePDT;
import org.apache.tinkerpop.gremlin.structure.io.pdt.CompositePDTAdapter;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PDTRegistry;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.util.DatetimeHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.util.CollectionUtil.asMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class GremlinLangTest {

    private static final GraphTraversalSource g = traversal().with(EmptyGraph.instance());

    @Parameterized.Parameter(value = 0)
    public Traversal traversal;

    @Parameterized.Parameter(value = 1)
    public String expected;

    @Test
    public void doTest() {
        final GremlinLang gremlinLang = traversal.asAdmin().getGremlinLang();
        assertEquals(expected, gremlinLang.getGremlin());
        assertFalse(gremlinLang.containsUnsupportedTypes());
        assertEquals("", gremlinLang.getUnsupportedType());
    }

    private static GraphTraversalSource newG() {
        return traversal().with(EmptyGraph.instance());
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {
        return Arrays.asList(new Object[][]{
                {g.V().count(), "g.V().count()"},
                {g.addV("test"), "g.addV(\"test\")"},
                {g.addV("t\"'est"), "g.addV(\"t\\\"'est\")"},
                {g.addV("dog", "pet"), "g.addV(\"dog\",\"pet\")"},
                {g.addV("dog", "pet", "animal"), "g.addV(\"dog\",\"pet\",\"animal\")"},
                {g.addV(__.constant("a"), __.constant("b")), "g.addV(__.constant(\"a\"),__.constant(\"b\"))"},
                {g.addV(__.constant("a"), __.constant("b"), __.constant("c")), "g.addV(__.constant(\"a\"),__.constant(\"b\"),__.constant(\"c\"))"},
                {g.inject(true, (byte) 1, (short) 2, 3, 4L, 5f, 6d, BigInteger.valueOf(7L), BigDecimal.valueOf(8L)),
                        "g.inject(true,1B,2S,3,4L,5.0F,6.0D,7N,8M)"},
                {g.inject(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY),
                        "g.inject(+Infinity,-Infinity)"},
                {g.inject(DatetimeHelper.parse("2018-03-21T08:35:44.741Z")),
                        "g.inject(datetime(\"2018-03-21T08:35:44.741Z\"))"},
                {g.inject(Date.from(DatetimeHelper.parse("2018-03-21T08:35:44.741Z").toInstant())),
                        "g.inject(datetime(\"2018-03-21T08:35:44.741Z\"))"},
                {g.inject(asMap("age", VertexProperty.Cardinality.list(33))),
                        "g.inject([\"age\":Cardinality.list(33)])"},
                {g.inject(new HashMap<>()), "g.inject([:])"},
                {g.inject(UUID.fromString("bfa9bbe8-c3a3-4017-acc3-cd02dda55e3e")), "g.inject(UUID(\"bfa9bbe8-c3a3-4017-acc3-cd02dda55e3e\"))"},
                {g.V(1).out("knows").values("name"), "g.V(1).out(\"knows\").values(\"name\")"},
                {g.V().has(T.label, "person"), "g.V().has(T.label,\"person\")"},
                {g.addE("knows").from(new DetachedVertex(1, "test1", Collections.emptyList())).to(new DetachedVertex(6, "test2", Collections.emptyList())),
                        "g.addE(\"knows\").from(__.V(1)).to(__.V(6))"},
                {g.V().hasId(P.within(Collections.emptyList())).count(), "g.V().hasId(P.within([])).count()"},
                {g.V(1).outE().has("weight", P.inside(0.0, 0.6)), "g.V(1).outE().has(\"weight\",P.gt(0.0D).and(P.lt(0.6D)))"},
                {g.withSack(1.0, Operator.sum).V(1).local(__.out("knows").barrier(SackFunctions.Barrier.normSack)).in("knows").barrier().sack(),
                        "g.withSack(1.0D,Operator.sum).V(1).local(__.out(\"knows\").barrier(Barrier.normSack)).in(\"knows\").barrier().sack()"},
                {g.inject(Arrays.asList(1, 2, 3)).skip(local, 1), "g.inject([1,2,3]).skip(Scope.local,1L)"},
                {g.V().repeat(both()).times(10), "g.V().repeat(__.both()).times(10)"},
                {g.V().has("name", "marko").
                        project("name", "friendsNames").
                        by("name").
                        by(out("knows").values("name").fold()),
                        "g.V().has(\"name\",\"marko\")." +
                                "project(\"name\",\"friendsNames\")." +
                                "by(\"name\")." +
                                "by(__.out(\"knows\").values(\"name\").fold())"},
                {g.inject(new int[]{5, 6}).union(__.V(Arrays.asList(1, 2)), __.V(Arrays.asList(3L, new int[]{4}))),
                        "g.inject([5,6]).union(__.V([1,2]),__.V([3L,[4]]))"},
                {g.with("evaluationTimeout", 1000).V(), "g.V()"},
                {g.withSideEffect("a", 1).V(), "g.withSideEffect(\"a\",1).V()"},
                {g.withStrategies(ReadOnlyStrategy.instance()).V(), "g.withStrategies(ReadOnlyStrategy).V()"},
                {g.withoutStrategies(ReadOnlyStrategy.class).V(), "g.withoutStrategies(ReadOnlyStrategy).V()"},
                {g.withStrategies(SeedStrategy.build().seed(999999).create()).V().order().by("name").coin(0.5),
                        "g.withStrategies(new SeedStrategy(seed:999999L)).V().order().by(\"name\").coin(0.5D)"},
                {g.withStrategies(SubgraphStrategy.build().vertices(hasLabel("person")).create()).V(),
                        "g.withStrategies(new SubgraphStrategy(checkAdjacentVertices:true,vertices:__.hasLabel(\"person\"))).V()",},
                {g.withStrategies(SubgraphStrategy.build().vertices(__.has("name", P.within("josh", "lop", "ripple"))).create()).V(),
                        "g.withStrategies(new SubgraphStrategy(checkAdjacentVertices:true,vertices:__.has(\"name\",P.within([\"josh\",\"lop\",\"ripple\"])))).V()"},
                {g.inject(GValue.of("x", "x")).V(GValue.of("ids", new int[]{1, 2, 3})), "g.inject(x).V(ids)"},
                {newG().inject(GValue.of(null, "test1"), GValue.of("xx2", "test2")), "g.inject(\"test1\",xx2)"},
                {newG().inject(new HashSet<>(Arrays.asList(1, 2))), "g.inject({1,2})"},
                // Character - single quote (no feature equivalent since grammar uses double quotes in feature files)
                {g.inject('\''), "g.inject(\"'\"c)"},
				// Character - backslash (not in feature file due to Gherkin escaping issues)
                {g.inject('\\'), "g.inject(\"\\\\\"c)"},
                // Binary - byte[] type (distinct from ByteBuffer)
                {g.inject(new byte[]{1, 2, 3}), "g.inject(Binary(\"AQID\"))"},
                {g.inject(new byte[]{}), "g.inject(Binary(\"\"))"},
                {g.inject(new byte[]{0}), "g.inject(Binary(\"AA==\"))"},
                // PDT
                {g.inject(new CompositePDT("MyType", asMap("x", 1, "y", "hello"))),
                        "g.inject(PDT(\"MyType\",[\"x\":1,\"y\":\"hello\"]))"},
                {g.inject(new CompositePDT("Empty", Collections.emptyMap())),
                        "g.inject(PDT(\"Empty\",[:]))"},
                // PDT with special characters in name
                {g.inject(new CompositePDT("say\"hello\"", asMap("v", 1))),
                        "g.inject(PDT(\"say\\\"hello\\\"\",[\"v\":1]))"},
                {g.inject(new CompositePDT("back\\slash", asMap("v", 1))),
                        "g.inject(PDT(\"back\\\\slash\",[\"v\":1]))"},
                // Nested PDT
                {g.inject(new CompositePDT("Outer", asMap("inner", new CompositePDT("Inner", asMap("v", 1))))),
                        "g.inject(PDT(\"Outer\",[\"inner\":PDT(\"Inner\",[\"v\":1])]))"},
                // match(String) — declarative pattern match spawn, no params
                {g.match("MATCH (p:person)"), "g.match(\"MATCH (p:person)\")"},
                // match(String, Map) — string params map is serialised inline in the gremlin string
                {g.match("MATCH (p:person {name: $who})-[:knows]->(f:person)", asMap("who", "marko")),
                        "g.match(\"MATCH (p:person {name: $who})-[:knows]->(f:person)\",[\"who\":\"marko\"])"},
                {g.match("MATCH (p:person {age: $age})-[:knows]->(f:person)", asMap("age", 29)),
                        "g.match(\"MATCH (p:person {age: $age})-[:knows]->(f:person)\",[\"age\":29])"},
                // Primitive PDT
                {g.inject(new PrimitivePDT("Uint32", "42")),
                        "g.inject(PDT(\"Uint32\",\"42\"))"},
                {g.inject(new PrimitivePDT("Empty", "")),
                        "g.inject(PDT(\"Empty\",\"\"))"},
        });
    }

    public static class ParameterTests {

        @Test(expected = IllegalArgumentException.class)
        public void shouldCheckParameterNameDontNeedEscaping() {
            g.V(GValue.of("\"", new int[]{1, 2, 3}));
        }

        @Test(expected = IllegalArgumentException.class)
        public void shouldCheckParameterNameIsNotNumber() {
            g.V(GValue.of("1", new int[]{1, 2, 3}));
        }

        @Test(expected = IllegalArgumentException.class)
        public void shouldCheckParameterNameIsValidIdentifier() {
            g.V(GValue.of("1a", new int[]{1, 2, 3}));
        }

        @Test
        public void shouldAllowParameterNameStartingWithUnderscore() {
            final GremlinLang gremlin = g.V(GValue.of("_1", new int[]{1, 2, 3})).asAdmin().getGremlinLang();
            assertEquals("g.V(_1)", gremlin.getGremlin());
        }

        @Test(expected = IllegalArgumentException.class)
        public void shouldNowAllowParameterNameDuplicates() {
            final GremlinLang gremlin = g.inject(GValue.of("ids", new int[]{1, 2})).V(GValue.of("ids", new int[]{2, 3}))
                    .asAdmin().getGremlinLang();
        }

        @Test
        public void shouldAllowToUseSameParameterTwice() {
            final int[] value = new int[]{1, 2, 3};
            final GValue<int[]> gValue = GValue.of("ids", value);
            final GremlinLang gremlin = g.inject(gValue).V(gValue).asAdmin().getGremlinLang();

            assertEquals("g.inject(ids).V(ids)", gremlin.getGremlin());
            assertEquals(value, gremlin.getParameters().get("ids"));
        }
    }

    public static class BinaryTests {
        @Test
        public void shouldEncodeByteBufferWithPositionOffset() {
            final ByteBuffer buf = ByteBuffer.allocate(4);
            buf.put((byte) 0);
            buf.put((byte) 1);
            buf.put((byte) 2);
            buf.put((byte) 3);
            buf.flip();
            buf.get(); // advance position by 1, remaining is [1, 2, 3]
            final String gremlin = g.inject(buf).asAdmin().getGremlinLang().getGremlin();
            assertEquals("g.inject(Binary(\"AQID\"))", gremlin);
        }
    }

    public static class ParameterStringTests {

        @Test
        public void shouldSerializeEmptyParameterMap() {
            assertEquals("[:]", GremlinLang.convertParametersToString(new HashMap<>()));
            assertEquals("[:]", GremlinLang.convertParametersToString(null));
        }

        @Test
        public void shouldSerializeSingleStringParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("name", "marko");
            assertEquals("[\"name\":\"marko\"]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeSingleIntegerParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", 1);
            assertEquals("[\"x\":1]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeLongParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", 1L);
            assertEquals("[\"x\":1L]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeDoubleParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", 1.5D);
            assertEquals("[\"x\":1.5D]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeBooleanParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("flag", true);
            assertEquals("[\"flag\":true]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeNullParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", null);
            assertEquals("[\"x\":null]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeUuidParameter() {
            final Map<String, Object> params = new HashMap<>();
            final UUID uuid = UUID.fromString("bfa9bbe8-c3a3-4017-acc3-cd02dda55e3e");
            params.put("id", uuid);
            assertEquals("[\"id\":UUID(\"bfa9bbe8-c3a3-4017-acc3-cd02dda55e3e\")]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeStringWithSpecialCharacters() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", "hello \"world\"");
            assertEquals("[\"x\":\"hello \\\"world\\\"\"]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeByteParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", (byte) 1);
            assertEquals("[\"x\":1B]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeShortParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", (short) 1);
            assertEquals("[\"x\":1S]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeBigIntegerParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", BigInteger.valueOf(123));
            assertEquals("[\"x\":123N]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeBigDecimalParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", BigDecimal.valueOf(1.5));
            assertEquals("[\"x\":1.5M]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeFloatParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", 1.5F);
            assertEquals("[\"x\":1.5F]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeCharacterParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", 'a');
            assertEquals("[\"x\":\"a\"c]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeDurationParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", Duration.ofHours(2).plusMinutes(30));
            assertEquals("[\"x\":Duration(9000,0)]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeBinaryParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", ByteBuffer.wrap(new byte[]{1, 2, 3}));
            assertEquals("[\"x\":Binary(\"AQID\")]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeEnumParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", T.id);
            assertEquals("[\"x\":T.id]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeGetParametersAsString() {
            final GraphTraversalSource g2 = newG();
            final String result = g2.V(GValue.of("x", 1)).
                    has("name", GValue.of("name", "marko")).
                    asAdmin().
                    getGremlinLang().
                    getParametersAsString();
            // parameters are in a HashMap so order may vary, just check structure
            assertTrue(result.startsWith("["));
            assertTrue(result.endsWith("]"));
            assertTrue(result, result.contains("\"x\":1"));
            assertTrue(result, result.contains("\"name\":\"marko\""));
        }

        @Test
        public void shouldSerializeOffsetDateTimeParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", DatetimeHelper.parse("2018-03-21T08:35:44.741Z"));
            assertEquals("[\"x\":datetime(\"2018-03-21T08:35:44.741Z\")]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeDateParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", Date.from(DatetimeHelper.parse("2018-03-21T08:35:44.741Z").toInstant()));
            assertEquals("[\"x\":datetime(\"2018-03-21T08:35:44.741Z\")]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeMultipleParameters() {
            // use a LinkedHashMap to guarantee order
            final Map<String, Object> params = new LinkedHashMap<>();
            params.put("x", 1);
            params.put("name", "marko");
            params.put("flag", true);
            assertEquals("[\"x\":1,\"name\":\"marko\",\"flag\":true]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeMapValuedParameter() {
            final Map<String, Object> params = new LinkedHashMap<>();
            final Map<Object, Object> nested = new LinkedHashMap<>();
            nested.put(T.label, "person");
            nested.put("name", "marko");
            params.put("m", nested);
            assertEquals("[\"m\":[(T.label):\"person\",\"name\":\"marko\"]]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeListValuedParameter() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", Arrays.asList(1, 2, 3));
            assertEquals("[\"x\":[1,2,3]]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeSetValuedParameter() {
            // use a LinkedHashSet to guarantee order
            final java.util.Set<Integer> set = new java.util.LinkedHashSet<>(Arrays.asList(1, 2));
            final Map<String, Object> params = new HashMap<>();
            params.put("x", set);
            assertEquals("[\"x\":{1,2}]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeStringWithBackslash() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", "path\\to\\file");
            assertEquals("[\"x\":\"path\\\\to\\\\file\"]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldSerializeStringWithUnicode() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", "caf\u00E9");
            assertEquals("[\"x\":\"caf\\u00E9\"]", GremlinLang.convertParametersToString(params));
        }

        @Test
        public void shouldHandleEmptyStringKey() {
            final Map<String, Object> params = new HashMap<>();
            params.put("", 1);
            final String result = GremlinLang.convertParametersToString(params);
            assertEquals("[\"\":1]", result);
        }

        @Test
        public void shouldHandleKeyWithSpaces() {
            final Map<String, Object> params = new HashMap<>();
            params.put("my key", 1);
            final String result = GremlinLang.convertParametersToString(params);
            assertEquals("[\"my key\":1]", result);
        }

        @Test
        public void shouldHandleNullKey() {
            final Map<String, Object> params = new HashMap<>();
            params.put(null, 1);
            final String result = GremlinLang.convertParametersToString(params);
            assertEquals("[null:1]", result);
        }

        @Test
        public void shouldSerializeUnicodeKey() {
            final Map<String, Object> params = new HashMap<>();
            params.put("caf\u00e9", 1);
            assertEquals("[\"caf\\u00E9\":1]", GremlinLang.convertParametersToString(params));
        }
    }

    public static class AdapterPrecedenceTests {

        /**
         * A type annotated with @ProviderDefined that also has an explicit adapter registered.
         * The adapter should take precedence over the annotation.
         */
        @ProviderDefined(name = "AnnotationName")
        private static class DualType {
            public int value = 42;

            private DualType() {}
            DualType(final int value) { this.value = value; }
        }

        @Test
        public void shouldUseAdapterOverAnnotation() {
            final PDTRegistry registry = PDTRegistry.empty();
            registry.register(new CompositePDTAdapter<DualType>() {
                @Override public String typeName() { return "AdapterName"; }
                @Override public Class<DualType> targetClass() { return DualType.class; }
                @Override public Map<String, Object> toFields(final DualType obj) {
                    return Collections.singletonMap("v", obj.value);
                }
                @Override public DualType fromFields(final Map<String, Object> fields) {
                    return new DualType((int) fields.get("v"));
                }
            });

            final GraphTraversalSource g2 = traversal().with(EmptyGraph.instance());
            g2.getGremlinLang().setPdtRegistry(registry);
            final String gremlin = g2.inject(new DualType(7)).asAdmin().getGremlinLang().getGremlin();

            // adapter produces "AdapterName" with key "v", not annotation's "AnnotationName" with key "value"
            assertTrue(gremlin, gremlin.contains("PDT(\"AdapterName\""));
            assertTrue(gremlin, gremlin.contains("\"v\":7"));
            assertFalse(gremlin, gremlin.contains("AnnotationName"));
        }

        private static class TestPoint {
            final int x;
            final int y;
            TestPoint(int x, int y) { this.x = x; this.y = y; }
        }

        @Test
        public void shouldDehydrateRegisteredTypeNestedInsideUnregisteredOuterPdt() {
            final PDTRegistry registry = PDTRegistry.empty();
            registry.register(new CompositePDTAdapter<TestPoint>() {
                @Override public String typeName() { return "Point"; }
                @Override public Class<TestPoint> targetClass() { return TestPoint.class; }
                @Override public Map<String, Object> toFields(final TestPoint obj) {
                    final Map<String, Object> m = new HashMap<>();
                    m.put("x", obj.x);
                    m.put("y", obj.y);
                    return m;
                }
                @Override public TestPoint fromFields(final Map<String, Object> fields) {
                    return new TestPoint((int) fields.get("x"), (int) fields.get("y"));
                }
            });

            // Outer is a raw CompositePDT whose "location" field value is a registered domain object
            final Map<String, Object> outerFields = new LinkedHashMap<>();
            outerFields.put("location", new TestPoint(3, 7));
            final CompositePDT outerPdt = new CompositePDT("Container", outerFields);

            final GraphTraversalSource g2 = traversal().with(EmptyGraph.instance());
            g2.getGremlinLang().setPdtRegistry(registry);
            final String gremlin = g2.inject(outerPdt).asAdmin().getGremlinLang().getGremlin();

            assertEquals("g.inject(PDT(\"Container\",[\"location\":PDT(\"Point\",[\"x\":3,\"y\":7])]))", gremlin);
        }

        private static class Uint32 {
            final long value;
            Uint32(final long value) { this.value = value; }
        }

        @Test
        public void shouldDehydratePrimitiveRegisteredType() {
            final PDTRegistry registry = PDTRegistry.empty();
            registry.register(new PrimitivePDTAdapter<Uint32>() {
                @Override public String typeName() { return "Uint32"; }
                @Override public Class<Uint32> targetClass() { return Uint32.class; }
                @Override public String toValue(final Uint32 obj) { return String.valueOf(obj.value); }
                @Override public Uint32 fromValue(final String value) { return new Uint32(Long.parseLong(value)); }
            });

            final GraphTraversalSource g2 = traversal().with(EmptyGraph.instance());
            g2.getGremlinLang().setPdtRegistry(registry);
            final String gremlin = g2.inject(new Uint32(99)).asAdmin().getGremlinLang().getGremlin();

            assertEquals("g.inject(PDT(\"Uint32\",\"99\"))", gremlin);
        }
    }

    public static class UnsupportedTypeTests {

        /**
         * A test-only type that will never have gremlin-lang literal support.
         */
        private static class UnsupportedType {
            @Override
            public String toString() {
                return "custom-unsupported";
            }
        }

        @Test
        public void shouldMarkUnsupportedTypeAndUseToString() {
            final UnsupportedType custom = new UnsupportedType();
            final GremlinLang gremlinLang = g.inject(custom).asAdmin().getGremlinLang();
            assertTrue(gremlinLang.containsUnsupportedTypes());
            assertEquals("UnsupportedType", gremlinLang.getUnsupportedType());
            assertTrue(gremlinLang.getGremlin().contains(custom.toString()));
        }

        @Test
        public void shouldNotAddUnsupportedTypeToParameterMap() {
            final GremlinLang gremlinLang = g.inject(new UnsupportedType()).asAdmin().getGremlinLang();
            // unsupported type is flagged but not stored in the parameter map
            assertTrue(gremlinLang.containsUnsupportedTypes());
            assertTrue(gremlinLang.getParameters().isEmpty());
        }

        @Test
        public void shouldRecordLastUnsupportedType() {
            final GremlinLang gremlinLang = g.inject(new UnsupportedType(), new Thread()).asAdmin().getGremlinLang();
            assertTrue(gremlinLang.containsUnsupportedTypes());
            // last one wins
            assertEquals("Thread", gremlinLang.getUnsupportedType());
        }

        @Test
        public void shouldPropagateFlagFromChildTraversal() {
            final GremlinLang gremlinLang = g.V().where(__.has("x", P.eq(new UnsupportedType()))).asAdmin().getGremlinLang();
            assertTrue(gremlinLang.containsUnsupportedTypes());
            assertEquals("UnsupportedType", gremlinLang.getUnsupportedType());
        }

        @Test
        public void shouldNotPropagateFlagFromCleanChildTraversal() {
            final GremlinLang gremlinLang = g.V().where(__.has("name", "marko")).asAdmin().getGremlinLang();
            assertFalse(gremlinLang.containsUnsupportedTypes());
            assertEquals("", gremlinLang.getUnsupportedType());
        }

        @Test
        public void shouldDetectUnsupportedTypeInList() {
            final GremlinLang gremlinLang = g.inject(Arrays.asList(1, new UnsupportedType())).asAdmin().getGremlinLang();
            assertTrue(gremlinLang.containsUnsupportedTypes());
            assertEquals("UnsupportedType", gremlinLang.getUnsupportedType());
        }

        @Test
        public void shouldDetectUnsupportedTypeInMap() {
            final Map<String, Object> map = new HashMap<>();
            map.put("key", new UnsupportedType());
            final GremlinLang gremlinLang = g.inject(map).asAdmin().getGremlinLang();
            assertTrue(gremlinLang.containsUnsupportedTypes());
            assertEquals("UnsupportedType", gremlinLang.getUnsupportedType());
        }

        @Test
        public void shouldDetectUnsupportedTypeInSet() {
            final java.util.Set<Object> set = new java.util.LinkedHashSet<>();
            set.add(1);
            set.add(new UnsupportedType());
            final GremlinLang gremlinLang = g.inject(set).asAdmin().getGremlinLang();
            assertTrue(gremlinLang.containsUnsupportedTypes());
            assertEquals("UnsupportedType", gremlinLang.getUnsupportedType());
        }

        @Test
        public void shouldDetectUnsupportedTypeInPredicate() {
            final GremlinLang gremlinLang = g.V().has("x", P.eq(new UnsupportedType())).asAdmin().getGremlinLang();
            assertTrue(gremlinLang.containsUnsupportedTypes());
            assertEquals("UnsupportedType", gremlinLang.getUnsupportedType());
        }

        @Test
        public void shouldPreserveUnsupportedTypeOnClone() {
            final GremlinLang original = g.inject(new UnsupportedType()).asAdmin().getGremlinLang();
            final GremlinLang cloned = original.clone();
            assertTrue(cloned.containsUnsupportedTypes());
            assertEquals("UnsupportedType", cloned.getUnsupportedType());
        }

        @Test
        public void shouldPreserveCleanStateOnClone() {
            final GremlinLang original = g.V(1).asAdmin().getGremlinLang();
            final GremlinLang cloned = original.clone();
            assertFalse(cloned.containsUnsupportedTypes());
            assertEquals("", cloned.getUnsupportedType());
        }

        @Test
        public void shouldStillAddGValueToParameterMap() {
            final GremlinLang gremlinLang = g.V(GValue.of("myId", 1)).asAdmin().getGremlinLang();
            assertFalse(gremlinLang.containsUnsupportedTypes());
            assertEquals(1, gremlinLang.getParameters().size());
            assertEquals(1, gremlinLang.getParameters().get("myId"));
        }

        @Test
        public void shouldDetectUnsupportedTypeForReferenceEdge() {
            final GremlinLang gremlinLang = newG().E(
                    new ReferenceEdge(1, "test label", new ReferenceVertex(1, "v1"), new ReferenceVertex(1, "v1")))
                    .asAdmin().getGremlinLang();
            assertTrue(gremlinLang.containsUnsupportedTypes());
            assertEquals("ReferenceEdge", gremlinLang.getUnsupportedType());
            assertEquals("g.E(e[1][1-test label->1])", gremlinLang.getGremlin());
        }

        @Test
        public void shouldDetectUnsupportedTypeInMapKey() {
            final Map<Object, Object> map = new LinkedHashMap<>();
            map.put(new UnsupportedType(), "value");
            final GremlinLang gremlinLang = g.inject(map).asAdmin().getGremlinLang();
            assertTrue(gremlinLang.containsUnsupportedTypes());
            assertEquals("UnsupportedType", gremlinLang.getUnsupportedType());
        }

        @Test(expected = IllegalArgumentException.class)
        public void shouldRejectUnsupportedTypeInConvertParametersToString() {
            final Map<String, Object> params = new HashMap<>();
            params.put("x", new UnsupportedType());
            GremlinLang.convertParametersToString(params);
        }

        @Test(expected = IllegalArgumentException.class)
        public void shouldRejectUnsupportedTypeInGValueViaGetParametersAsString() {
            final GremlinLang gremlinLang = g.V(GValue.of("x", new UnsupportedType())).asAdmin().getGremlinLang();
            // named GValue stores the raw value in the parameter map without type-checking,
            // so containsUnsupportedTypes() does not detect it
            assertFalse(gremlinLang.containsUnsupportedTypes());
            // the error is caught when the parameter map is serialized
            gremlinLang.getParametersAsString();
        }

        @Test
        public void shouldDetectUnsupportedTypeInUnnamedGValue() {
            // unnamed GValue (null name) recurses into argAsString(gValue.get()),
            // which hits the unsupported-type fallback and sets the flag
            final GremlinLang gremlinLang = g.inject(GValue.of(null, new UnsupportedType())).asAdmin().getGremlinLang();
            assertTrue(gremlinLang.containsUnsupportedTypes());
            assertEquals("UnsupportedType", gremlinLang.getUnsupportedType());
        }

        @Test
        public void shouldPersistFlagAfterSupportedTypeFollows() {
            final GremlinLang gremlinLang = g.inject(new UnsupportedType(), "hello").asAdmin().getGremlinLang();
            assertTrue(gremlinLang.containsUnsupportedTypes());
            assertEquals("UnsupportedType", gremlinLang.getUnsupportedType());
        }
    }
}
