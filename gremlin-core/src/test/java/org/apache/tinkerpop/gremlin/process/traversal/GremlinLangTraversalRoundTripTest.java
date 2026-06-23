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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinAntlrToJava;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinQueryParser;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;

/**
 * Parameterized round-trip test for GremlinLang serialization of traversal-bearing arguments.
 * Each test case serializes a traversal to GremlinLang, parses it back via the ANTLR grammar,
 * and verifies the resulting GremlinLang string is structurally identical.
 * <p>
 * This complements the Gherkin feature tests (which verify execution semantics) by ensuring that
 * the serialization/deserialization layer preserves all traversal argument forms correctly.
 */
@RunWith(Parameterized.class)
public class GremlinLangTraversalRoundTripTest {

    private static final GraphTraversalSource g = traversal().with(EmptyGraph.instance());

    @Parameterized.Parameter
    public Traversal<?, ?> traversal;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // === has() with traversal values ===
                {g.V().has("name", __.values("x"))},
                {g.V().has("name", __.constant("marko"))},
                {g.V().has("name", __.V().values("name"))},
                {g.V().has("person", "name", __.values("x"))},

                // === has() with predicate-wrapped traversals ===
                {g.V().has("name", P.eq(__.values("x").asAdmin()))},
                {g.V().has("age", P.gt(__.constant(30).asAdmin()))},
                {g.V().has("age", P.lt(__.V().values("age").asAdmin()))},
                {g.V().has("age", P.gte(__.constant(18).asAdmin()))},
                {g.V().has("age", P.lte(__.constant(65).asAdmin()))},
                {g.V().has("age", P.neq(__.constant(0).asAdmin()))},

                // === ConnectiveP with traversals ===
                {g.V().has("age", P.gt(__.constant(10).asAdmin()).and(P.lt(__.constant(30).asAdmin())))},
                {g.V().has("age", P.lt(__.constant(20).asAdmin()).or(P.gt(__.constant(50).asAdmin())))},
                {g.V().has("age", P.gt(__.constant(10).asAdmin()).and(P.lt(__.constant(30).asAdmin())).or(P.eq(__.constant(99).asAdmin())))},

                // === within/without with traversals ===
                {g.V().has("name", P.within(__.V().values("name").fold().asAdmin()))},
                {g.V().has("name", P.without(__.V().values("name").fold().asAdmin()))},
                {g.V().has("name", P.within(__.constant("a").asAdmin(), __.constant("b").asAdmin()))},

                // === not() with traversal ===
                {g.V().has("name", __.not(__.identity()))},

                // === hasId/hasLabel/hasKey/hasValue with traversals ===
                {g.V().hasId(__.V().id())},
                {g.V().hasId(P.eq(__.V().id().asAdmin()))},
                {g.V().hasLabel(__.V().label())},
                {g.V().properties("age").hasKey(__.constant("age"))},
                {g.V().properties("age").hasValue(__.constant(29))},

                // === is() with traversals ===
                {g.V().values("age").is(__.constant(29))},
                {g.V().values("age").is(P.gt(__.V().values("age").asAdmin()))},
                {g.V().values("age").is(P.within(__.V().values("age").fold().asAdmin()))},

                // === V() and E() with traversals ===
                {g.V().V(__.select("ids"))},
                {g.V().V(__.out("knows").id())},
                {g.V().E(__.select("edgeIds"))},
                {g.V().E(__.outE().id())},
                {g.V(__.V().id())},
                {g.E(__.V().outE().id())},

                // === property() with traversals ===
                {g.V().property("key", __.select("val"))},
                {g.V().property("key", __.V().values("name"))},
                {g.V().property("count", __.out().count())},
                {g.V().property(Cardinality.list, "x", __.out().values("name"))},
                {g.V().property(__.V().project("a").by("name"))},

                // === where() with traversal predicates ===
                {g.V().values("age").where(P.gt(__.V().values("age").asAdmin()))},
                {g.V().values("age").where(P.within(__.V().values("age").fold().asAdmin()))},

                // === Backward compatibility: literal arguments still round-trip ===
                {g.V().has("name", "marko")},
                {g.V().has("name", P.eq("marko"))},
                {g.V().has("age", P.gt(30))},
                {g.V().has("name", P.within("a", "b", "c"))},
                {g.V(1)},
                {g.V(1, 2, 3)},
        });
    }

    @Test
    public void shouldRoundTrip() {
        final String originalGremlin = traversal.asAdmin().getGremlinLang().getGremlin();
        final GremlinAntlrToJava antlr = new GremlinAntlrToJava();
        final Object parsed = GremlinQueryParser.parse(originalGremlin, antlr);
        final String roundTrippedGremlin = ((Traversal<?, ?>) parsed).asAdmin().getGremlinLang().getGremlin();
        assertEquals("GremlinLang round-trip failed for: " + originalGremlin,
                originalGremlin, roundTrippedGremlin);
    }
}
