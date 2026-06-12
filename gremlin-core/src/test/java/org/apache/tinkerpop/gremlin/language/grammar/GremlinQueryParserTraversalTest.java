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
package org.apache.tinkerpop.gremlin.language.grammar;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Before;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;

/**
 * Tests that the Gremlin grammar correctly parses the new traversal-accepting syntax forms
 * for has(), V(), E(), property(), and predicates.
 */
public class GremlinQueryParserTraversalTest {

    private GraphTraversalSource g;
    private GremlinAntlrToJava antlrToLanguage;

    @Before
    public void setup() {
        g = traversal().withEmbedded(EmptyGraph.instance());
        antlrToLanguage = new GremlinAntlrToJava();
    }

    private Object eval(final String query) {
        final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(query));
        final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
        return antlrToLanguage.visit(parser.queryList());
    }

    private void compare(final Object expected, final Object actual) {
        assertEquals(((DefaultGraphTraversal) expected).asAdmin().getGremlinLang(),
                ((DefaultGraphTraversal) actual).asAdmin().getGremlinLang());
    }

    // ---- has(key, traversal) ----

    @Test
    public void shouldParseHasStringTraversal() {
        compare(g.V().has("name", __.values("x")),
                eval("g.V().has('name', __.values('x'))"));
    }

    // ---- has(label, key, traversal) ----

    @Test
    public void shouldParseHasStringStringTraversal() {
        compare(g.V().has("person", "name", __.values("x")),
                eval("g.V().has('person', 'name', __.values('x'))"));
    }

    // ---- has(T, traversal) ----

    @Test
    public void shouldParseHasTTraversal() {
        compare(g.V().has(T.id, __.values("x")),
                eval("g.V().has(T.id, __.values('x'))"));
    }

    // ---- V(traversal) mid-traversal ----

    @Test
    public void shouldParseVTraversalMidTraversal() {
        compare(g.V().out().V(__.select("ids")),
                eval("g.V().out().V(__.select('ids'))"));
    }

    // ---- E(traversal) mid-traversal ----

    @Test
    public void shouldParseETraversalMidTraversal() {
        compare(g.V().out().E(__.select("ids")),
                eval("g.V().out().E(__.select('ids'))"));
    }

    // ---- property(key, traversal) ----

    @Test
    public void shouldParsePropertyKeyTraversal() {
        compare(g.V().property("key", __.select("val")),
                eval("g.V().property('key', __.select('val'))"));
    }

    // ---- property(Cardinality, key, traversal) ----

    @Test
    public void shouldParsePropertyCardinalityKeyTraversal() {
        compare(g.V().property(VertexProperty.Cardinality.single, "key", __.select("val")),
                eval("g.V().property(Cardinality.single, 'key', __.select('val'))"));
    }

    // ---- P.eq(traversal) via has ----

    @Test
    public void shouldParseHasWithPEqTraversal() {
        compare(g.V().has("name", P.eq(__.values("x"))),
                eval("g.V().has('name', P.eq(__.values('x')))"));
    }

    // ---- P.gt(traversal) via has ----

    @Test
    public void shouldParseHasWithPGtTraversal() {
        compare(g.V().has("age", P.gt(__.values("x"))),
                eval("g.V().has('age', P.gt(__.values('x')))"));
    }

    // ---- P.within(traversal) via has ----

    @Test
    public void shouldParseHasWithPWithinTraversal() {
        compare(g.V().has("name", P.within(__.values("x"))),
                eval("g.V().has('name', P.within(__.values('x')))"));
    }

    // ---- property(traversal) - Map-producing form ----

    @Test
    public void shouldParsePropertyTraversal() {
        compare(g.V().property(__.V().project("a").by("name")),
                eval("g.V().property(__.V().project('a').by('name'))"));
    }
}
