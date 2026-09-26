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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Before;
import org.junit.Test;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;

import java.util.HashMap;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.util.CollectionUtil.asMap;
import static org.junit.Assert.assertEquals;

public class TraversalSourceSpawnVisitorTest {

    private GraphTraversalSource g;
    private GremlinAntlrToJava antlrToLanguage;

    @Before
    public void setup()  {
        g = traversal().withEmbedded(EmptyGraph.instance());
        antlrToLanguage = new GremlinAntlrToJava();
    }
    
    private void compare(Object expected, Object actual) {
        assertEquals(expected.toString(), actual.toString());
    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_inject() {
        compare(g.inject(), eval("g.inject()"));
        compare(g.inject(null), eval("g.inject(null)"));
        compare(g.inject(null, null), eval("g.inject(null, null)"));
        compare(g.inject(null, 1), eval("g.inject(null, 1)"));
        compare(g.inject(1, null), eval("g.inject(1, null)"));
        compare(g.inject(1), eval("g.inject(1)"));
        compare(g.inject(1, 2, 3, 4), eval("g.inject(1,2,3,4)"));
        compare(g.inject(1, 2, 3, new HashMap<>()), eval("g.inject(1,2,3,[:])"));
    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_V() {
        compare(g.V().out().values("name").inject("daniel"), eval("g.V().out().values('name').inject('daniel')"));
        compare(g.V(4).out().values("name").inject("daniel"), eval("g.V(4).out().values('name').inject('daniel')"));
        compare(g.V(4, 5).out().values("name").inject("daniel"), eval("g.V(4, 5).out().values('name').inject('daniel')"));
    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_E() {
        compare(g.E().values("name").inject("daniel"), eval("g.E().values('name').inject('daniel')"));
        compare(g.E(4).values("name").inject("daniel"), eval("g.E(4).values('name').inject('daniel')"));
        compare(g.E(4, 5).values("name").inject("daniel"), eval("g.E(4, 5).values('name').inject('daniel')"));

    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_match() {
        compare(g.match("MATCH (p:person)"), eval("g.match(\"MATCH (p:person)\")"));
        compare(g.match("MATCH (a:person)-[:knows]->(b:person)"),
                eval("g.match(\"MATCH (a:person)-[:knows]->(b:person)\")"));
        // match(String, Map) — params map is embedded inline; compare via GremlinLang equality
        assertEquals(
                ((DefaultGraphTraversal) g.match("MATCH (p:person {name: $who})-[:knows]->(f:person)", asMap("who", "marko"))).asAdmin().getGremlinLang(),
                ((DefaultGraphTraversal) eval("g.match(\"MATCH (p:person {name: $who})-[:knows]->(f:person)\",[\"who\":\"marko\"])")).asAdmin().getGremlinLang());
        assertEquals(
                ((DefaultGraphTraversal) g.match("MATCH (p:person {age: $age})-[:knows]->(f:person)", asMap("age", 29))).asAdmin().getGremlinLang(),
                ((DefaultGraphTraversal) eval("g.match(\"MATCH (p:person {age: $age})-[:knows]->(f:person)\",[\"age\":29])")).asAdmin().getGremlinLang());
    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_addE() {
        compare(g.addE("knows"), eval("g.addE('knows')"));
    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_io() {
        compare(g.io("data.xml"), eval("g.io('data.xml')"));
    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_mergeV_Map() {
        compare(g.mergeV(new HashMap<>()), eval("g.mergeV([:])"));
    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_mergeV_Traversal() {
        compare(g.mergeV(__.constant(new HashMap<>())), eval("g.mergeV(__.constant([:]))"));
    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_mergeE_Map() {
        compare(g.mergeE(new HashMap<>()), eval("g.mergeE([:])"));
    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_mergeE_Traversal() {
        compare(g.mergeE(__.constant(new HashMap<>())), eval("g.mergeE(__.constant([:]))"));
    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_call_empty() {
        compare(g.call(), eval("g.call()"));
    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_call_string() {
        compare(g.call("svc"), eval("g.call('svc')"));
    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_call_string_map() {
        compare(g.call("svc", new HashMap<>()), eval("g.call('svc', [:])"));
    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_call_string_traversal() {
        compare(g.call("svc", __.identity()), eval("g.call('svc', __.identity())"));
    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_call_string_map_traversal() {
        compare(g.call("svc", new HashMap<>(), __.identity()), eval("g.call('svc', [:], __.identity())"));
    }

    @Test
    public void shouldParseTraversalSourceSpawnMethod_union() {
        compare(g.union((Traversal) __.V(), (Traversal) __.E()), eval("g.union(__.V(), __.E())"));
    }

    private Object eval(final String query) {
        final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(query));
        final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
        return antlrToLanguage.visit(parser.queryList());
    }
}
