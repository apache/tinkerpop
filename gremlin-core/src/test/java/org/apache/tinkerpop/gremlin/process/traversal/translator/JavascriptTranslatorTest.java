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

import org.apache.tinkerpop.gremlin.process.traversal.GraphOp;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
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
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JavascriptTranslatorTest {
    private static final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());
    private static final Translator.ScriptTranslator translator = JavascriptTranslator.of("g");

    @Test
    public void shouldTranslateStrategies() throws Exception {
        assertEquals("g.withStrategies(new ReadOnlyStrategy()," +
                        "new SubgraphStrategy({checkAdjacentVertices:false,vertices:__.hasLabel(\"person\")})," +
                        "new SeedStrategy({seed:999999})).V().has(\"name\")",
                translator.translate(g.withStrategies(ReadOnlyStrategy.instance(),
                        SubgraphStrategy.build().checkAdjacentVertices(false).vertices(hasLabel("person")).create(),
                        SeedStrategy.build().seed(999999).create()).
                        V().has("name").asAdmin().getBytecode()).getScript());
    }

    @Test
    public void shouldTranslateMaps() {
        final String script = translator.translate(g.V().id().is(new LinkedHashMap<Object,Object>() {{
            put(3, "32");
            put(Arrays.asList(1, 2, 3.1d), 4);
        }}).asAdmin().getBytecode()).getScript();
        assertEquals("g.V().id().is(new Map([[3,\"32\"],[[1, 2, 3.1],4]]))", script);
    }

    @Test
    public void shouldTranslateDate() {
        final Calendar c = Calendar.getInstance();
        c.set(1975, Calendar.SEPTEMBER, 7);
        final Date d = c.getTime();
        assertTranslation(String.format("new Date(%s)", d.getTime()), d);
    }

    @Test
    public void shouldTranslateUuid() {
        final UUID uuid = UUID.fromString("ffffffff-fd49-1e4b-0000-00000d4b8a1d");
        assertTranslation(String.format("'%s'", uuid), uuid);
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
        assertTranslation("Order.desc", Order.desc);
    }

    @Test
    public void shouldTranslatePop() {
        assertTranslation("Pop.last", Pop.last);
    }

    @Test
    public void shouldTranslateTextP() {
        assertTranslation("TextP.containing(\"ark\")", TextP.containing("ark"));
        assertTranslation("TextP.regex(\"ark\")", TextP.regex("ark"));
        assertTranslation("TextP.notRegex(\"ark\")", TextP.notRegex("ark"));
    }

    @Test
    public void shouldTranslateScope() {
        assertTranslation("Scope.local", Scope.local);
    }

    @Test
    public void shouldTranslateCardinalityValue() {
        assertTranslation("CardinalityValue.set(\"test\")", VertexProperty.Cardinality.set("test"));
    }

    @Test
    public void shouldHaveValidToString() {
        assertEquals("translator[h:gremlin-javascript]", JavascriptTranslator.of("h").toString());
    }

    @Test
    public void shouldHaveNull() {
        assertEquals("g.inject(null,null)", translator.translate(g.inject(null, null).asAdmin().getBytecode()).getScript());
        assertEquals("g.V()", translator.translate(g.V().asAdmin().getBytecode()).getScript());
        assertEquals("g.V(null)", translator.translate(g.V(null).asAdmin().getBytecode()).getScript());
        assertEquals("g.V(null,null)", translator.translate(g.V(null, null).asAdmin().getBytecode()).getScript());
        assertEquals("g.E()", translator.translate(g.E().asAdmin().getBytecode()).getScript());
        assertEquals("g.E(null)", translator.translate(g.E(null).asAdmin().getBytecode()).getScript());
        assertEquals("g.E(null,null)", translator.translate(g.E(null, null).asAdmin().getBytecode()).getScript());
    }

    @Test
    public void shouldEscapeStrings() {
        final String script = translator.translate(g.addV("customer")
                .property("customer_id", 501L)
                .property("name", "Foo\u0020Bar")
                .property("age", 25)
                .property("special", "`~!@#$%^&*()-_=+[{]}\\|;:'\",<.>/?")
                .asAdmin().getBytecode()).getScript();

        assertEquals("g.addV(\"customer\")" +
                        ".property(\"customer_id\",501)" +
                        ".property(\"name\",\"Foo Bar\")" +
                        ".property(\"age\",25)" +
                        ".property(\"special\",\"\"\"`~!@#\\$%^&*()-_=+[{]}\\\\|;:'\\\",<.>/?\"\"\")",
                script);
    }

    @Test
    public void shouldTranslateVertexAndEdge() {
        final Object id1 = "customer:10:foo\u0020bar\u0020\u0024100#90"; // customer:10:foo bar $100#90
        final Vertex vertex1 = DetachedVertex.build().setLabel("customer").setId(id1)
                .create();
        final String script1 = translator.translate(g.inject(vertex1).asAdmin().getBytecode()).getScript();
        assertEquals("g.inject(new Vertex(" +
                        "\"customer:10:foo bar \\$100#90\"," +
                        "\"customer\", null))",
                script1);

        final Object id2 = "user:20:foo\\u0020bar\\u005c\\u0022mr\\u005c\\u0022\\u00241000#50"; // user:20:foo\u0020bar\u005c\u0022mr\u005c\u0022\u00241000#50
        final Vertex vertex2 = DetachedVertex.build().setLabel("user").setId(id2)
                .create();
        final String script2 = translator.translate(g.inject(vertex2).asAdmin().getBytecode()).getScript();
        assertEquals("g.inject(new Vertex(" +
                        "\"user:20:foo\\\\u0020bar\\\\u005c\\\\u0022mr\\\\u005c\\\\u0022\\\\u00241000#50\"," +
                        "\"user\", null))",
                script2);

        final Object id3 = "knows:30:foo\u0020bar\u0020\u0024100:\\u0020\\u0024500#70";
        final Edge edge = DetachedEdge.build().setLabel("knows").setId(id3)
                .setOutV((DetachedVertex) vertex1)
                .setInV((DetachedVertex) vertex2)
                .create();
        final String script3 = translator.translate(g.inject(edge).asAdmin().getBytecode()).getScript();
        assertEquals("g.inject(" +
                        "new Edge(\"knows:30:foo bar \\$100:\\\\u0020\\\\u0024500#70\", " +
                        "new Vertex(\"customer:10:foo bar \\$100#90\",\"customer\", null)," +
                        "\"knows\", " +
                        "new Vertex(\"user:20:foo\\\\u0020bar\\\\u005c\\\\u0022mr\\\\u005c\\\\u0022\\\\u00241000#50\",\"user\",null)," +
                        "null))",
                script3);

        final String script4 = translator.translate(
                g.addE("knows").from(vertex1).to(vertex2).property("when", "2018/09/21")
                        .asAdmin().getBytecode()).getScript();
        assertEquals("g.addE(\"knows\")" +
                        ".from_(new Vertex(\"customer:10:foo bar \\$100#90\",\"customer\", null))" +
                        ".to(new Vertex(\"user:20:foo\\\\u0020bar\\\\u005c\\\\u0022mr\\\\u005c\\\\u0022\\\\u00241000#50\",\"user\", null))" +
                        ".property(\"when\",\"2018/09/21\")",
                script4);
    }

    @Test
    public void shouldTranslateTx() {
        String script = translator.translate(GraphOp.TX_COMMIT.getBytecode()).getScript();
        assertEquals("g.tx().commit()", script);
        script = translator.translate(GraphOp.TX_ROLLBACK.getBytecode()).getScript();
        assertEquals("g.tx().rollback()", script);
    }

    private void assertTranslation(final String expectedTranslation, final Object... objs) {
        final String script = translator.translate(g.inject(objs).asAdmin().getBytecode()).getScript();
        assertEquals(String.format("g.inject(%s)", expectedTranslation), script);
    }
}
