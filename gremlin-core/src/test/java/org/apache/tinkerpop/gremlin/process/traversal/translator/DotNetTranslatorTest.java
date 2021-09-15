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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.UUID;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.asc;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DotNetTranslatorTest {
    private static final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());
    private static final Translator.ScriptTranslator translator = DotNetTranslator.of("g");

    @Test
    public void shouldTranslateStrategies() throws Exception {
        assertEquals("g.WithStrategies(new ReadOnlyStrategy()," +
                        "new SubgraphStrategy(checkAdjacentVertices: false, vertices: __.HasLabel(\"person\"))).V().Has(\"name\")",
                translator.translate(g.withStrategies(ReadOnlyStrategy.instance(),
                        SubgraphStrategy.build().checkAdjacentVertices(false).vertices(hasLabel("person")).create()).
                        V().has("name").asAdmin().getBytecode()).getScript());
    }

    @Test
    public void shouldTranslateMaps() {
        final String script = translator.translate(g.V().id().is(new LinkedHashMap<Object,Object>() {{
            put(3, "32");
            put(Arrays.asList(1, 2, 3.1d), 4);
        }}).asAdmin().getBytecode()).getScript();
        assertEquals("g.V().Id().Is(new Dictionary<object,object> {{3, \"32\"}, {new List<object> {1, 2, 3.1}, 4}})", script);
    }

    @Test
    public void shouldTranslateValues() {
        final String script = translator.translate(g.V().values("name").asAdmin().getBytecode()).getScript();
        assertEquals("g.V().Values<object>(\"name\")", script);
    }

    @Test
    public void shouldTranslateValue() {
        final String script = translator.translate(g.V().properties().order().by(T.value, asc).value().asAdmin().getBytecode()).getScript();
        assertEquals("g.V().Properties<object>().Order().By(T.Value,Order.Asc).Value<object>()", script);
    }

    @Test
    public void shouldTranslateInject() {
        String script = translator.translate(g.inject(10,20,null,20,10,10).asAdmin().getBytecode()).getScript();
        assertEquals("g.Inject<object>(10,20,null,20,10,10)", script);
        script = translator.translate(g.inject().asAdmin().getBytecode()).getScript();
        assertEquals("g.Inject<object>()", script);
        script = translator.translate(g.inject((Object) null).asAdmin().getBytecode()).getScript();
        assertEquals("g.Inject<object>(null)", script);
        script = translator.translate(g.inject(null, null).asAdmin().getBytecode()).getScript();
        assertEquals("g.Inject<object>(null,null)", script);
        script = translator.translate(g.V().values("age").inject().asAdmin().getBytecode()).getScript();
        assertEquals("g.V().Values<object>(\"age\").Inject()", script);
        script = translator.translate(g.V().values("age").inject(null).asAdmin().getBytecode()).getScript();
        assertEquals("g.V().Values<object>(\"age\").Inject(null)", script);
        script = translator.translate(g.V().values("age").inject(null, null).asAdmin().getBytecode()).getScript();
        assertEquals("g.V().Values<object>(\"age\").Inject(null,null)", script);
    }

    @Test
    public void shouldTranslateGroup() {
        final String script = translator.translate(g.V().group("x").group().by("name").asAdmin().getBytecode()).getScript();
        assertEquals("g.V().Group(\"x\").Group<object,object>().By(\"name\")", script);
    }

    @Test
    public void shouldTranslateGroupCount() {
        final String script = translator.translate(g.V().groupCount("x").groupCount().by("name").asAdmin().getBytecode()).getScript();
        assertEquals("g.V().GroupCount(\"x\").GroupCount<object>().By(\"name\")", script);
    }

    @Test
    public void shouldTranslateDate() {
        final Calendar c = Calendar.getInstance();
        c.set(1975, Calendar.SEPTEMBER, 7);
        final Date d = c.getTime();
        assertTranslation(String.format("DateTimeOffset.FromUnixTimeMilliseconds(%s)", d.getTime()), d);
    }

    @Test
    public void shouldTranslateUuid() {
        final UUID uuid = UUID.fromString("ffffffff-fd49-1e4b-0000-00000d4b8a1d");
        assertTranslation(String.format("new Guid(\"%s\")", uuid), uuid);
    }

    @Test
    public void shouldTranslateP() {
        assertTranslation("P.Gt(1).And(P.Gt(2)).And(P.Gt(3))", P.gt(1).and(P.gt(2)).and(P.gt(3)));
    }

    @Test
    public void shouldTranslateColumn() {
        assertTranslation("Column.Keys", Column.keys);
    }

    @Test
    public void shouldTranslateDirection() {
        assertTranslation("Direction.Both", Direction.BOTH);
    }

    @Test
    public void shouldTranslateOrder() {
        assertTranslation("Order.Desc", Order.desc);
    }

    @Test
    public void shouldTranslatePop() {
        assertTranslation("Pop.Last", Pop.last);
    }

    @Test
    public void shouldTranslateScope() {
        assertTranslation("Scope.Local", Scope.local);
    }

    @Test
    public void shouldHaveValidToString() {
        assertEquals("translator[h:gremlin-dotnet]", DotNetTranslator.of("h").toString());
    }

    @Test
    public void shouldTranslateHasLabelNull() {
        String script = translator.translate(g.V().hasLabel(null).asAdmin().getBytecode()).getScript();
        assertEquals("g.V().HasLabel((string) null)", script);
        script = translator.translate(g.V().hasLabel(null, null).asAdmin().getBytecode()).getScript();
        assertEquals("g.V().HasLabel((string) null,(string) null)", script);
        script = translator.translate(g.V().hasLabel(null, "person").asAdmin().getBytecode()).getScript();
        assertEquals("g.V().HasLabel((string) null,\"person\")", script);
        script = translator.translate(g.V().hasLabel(null, "person", null).asAdmin().getBytecode()).getScript();
        assertEquals("g.V().HasLabel((string) null,\"person\",(string) null)", script);
        script = translator.translate(g.V().has(T.label, (Object) null).asAdmin().getBytecode()).getScript();
        assertEquals("g.V().Has(T.Label,(object) null)", script);
    }

    @Test
    public void shouldTranslateHasNull() {
        String script = translator.translate(g.V().has("k", (Object) null).asAdmin().getBytecode()).getScript();
        assertEquals("g.V().Has(\"k\",(object) null)", script);
        script = translator.translate(g.V().has("l", "k", (Object) null).asAdmin().getBytecode()).getScript();
        assertEquals("g.V().Has(\"l\",\"k\",(object) null)", script);
    }

    @Test
    public void shouldEscapeStrings() {
        final String script = translator.translate(g.addV("customer")
                .property("customer_id", 501L)
                .property("name", "Foo\u0020Bar")
                .property("age", 25)
                .property("special", "`~!@#$%^&*()-_=+[{]}\\|;:'\",<.>/?")
                .asAdmin().getBytecode()).getScript();

        assertEquals("g.AddV(\"customer\")" +
                        ".Property(\"customer_id\",501)" +
                        ".Property(\"name\",\"Foo Bar\")" +
                        ".Property(\"age\",25)" +
                        ".Property(\"special\",\"`~!@#$%^&*()-_=+[{]}\\\\|;:'\\\",<.>/?\")",
                script);
    }

    @Test
    public void shouldTranslateVertexAndEdge() {
        final Object id1 = "customer:10:foo\u0020bar\u0020\u0024100#90"; // customer:10:foo bar $100#90
        final Vertex vertex1 = DetachedVertex.build().setLabel("customer").setId(id1)
                .create();
        final String script1 = translator.translate(g.inject(vertex1).asAdmin().getBytecode()).getScript();
        assertEquals("g.Inject(new Vertex(" +
                        "\"customer:10:foo bar $100#90\", " +
                        "\"customer\"))",
                script1);

        final Object id2 = "user:20:foo\\u0020bar\\u005c\\u0022mr\\u005c\\u0022\\u00241000#50"; // user:20:foo\u0020bar\u005c\u0022mr\u005c\u0022\u00241000#50
        final Vertex vertex2 = DetachedVertex.build().setLabel("user").setId(id2)
                .create();
        final String script2 = translator.translate(g.inject(vertex2).asAdmin().getBytecode()).getScript();
        assertEquals("g.Inject(new Vertex(" +
                        "\"user:20:foo\\\\u0020bar\\\\u005c\\\\u0022mr\\\\u005c\\\\u0022\\\\u00241000#50\", " +
                        "\"user\"))",
                script2);

        final Object id3 = "knows:30:foo\u0020bar\u0020\u0024100:\\u0020\\u0024500#70";
        final Edge edge = DetachedEdge.build().setLabel("knows").setId(id3)
                .setOutV((DetachedVertex) vertex1)
                .setInV((DetachedVertex) vertex2)
                .create();
        final String script3 = translator.translate(g.inject(edge).asAdmin().getBytecode()).getScript();
        assertEquals("g.Inject(" +
                        "new Edge(\"knows:30:foo bar $100:\\\\u0020\\\\u0024500#70\", " +
                        "new Vertex(\"customer:10:foo bar $100#90\", \"customer\"), " +
                        "\"knows\", " +
                        "new Vertex(\"user:20:foo\\\\u0020bar\\\\u005c\\\\u0022mr\\\\u005c\\\\u0022\\\\u00241000#50\", \"user\")))",
                script3);

        final String script4 = translator.translate(
                g.addE("knows").from(vertex1).to(vertex2).property("when", "2018/09/21")
                        .asAdmin().getBytecode()).getScript();
        assertEquals("g.AddE(\"knows\")" +
                        ".From(new Vertex(\"customer:10:foo bar $100#90\", \"customer\"))" +
                        ".To(new Vertex(\"user:20:foo\\\\u0020bar\\\\u005c\\\\u0022mr\\\\u005c\\\\u0022\\\\u00241000#50\", \"user\"))" +
                        ".Property(\"when\",\"2018/09/21\")",
                script4);
    }

    private void assertTranslation(final String expectedTranslation, final Object... objs) {
        final String script = translator.translate(g.inject(objs).asAdmin().getBytecode()).getScript();
        assertEquals(String.format("g.Inject(%s)", expectedTranslation), script);
    }
}
