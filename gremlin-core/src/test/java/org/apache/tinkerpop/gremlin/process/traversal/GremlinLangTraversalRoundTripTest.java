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
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;

/**
 * Property 7: GremlinLang serialization round-trip preserves traversal arguments.
 * <p>
 * For any step containing a child traversal argument, serializing to GremlinLang and parsing back
 * SHALL produce a structurally equivalent traversal.
 * <p>
 * <b>Validates: Requirements 1.3, 3.6, 4.5, 6.5, 7.3</b>
 */
public class GremlinLangTraversalRoundTripTest {

    private static final GraphTraversalSource g = traversal().with(EmptyGraph.instance());

    /**
     * Serializes a traversal to GremlinLang, parses it back, and verifies structural equivalence
     * by comparing the GremlinLang output of both the original and the round-tripped traversal.
     */
    private void assertRoundTrip(final Traversal<?, ?> traversal) {
        final String originalGremlin = traversal.asAdmin().getGremlinLang().getGremlin();

        // Parse the GremlinLang string back into a traversal
        final GremlinAntlrToJava antlr = new GremlinAntlrToJava();
        final Object parsed = GremlinQueryParser.parse(originalGremlin, antlr);

        // Get the GremlinLang of the parsed traversal
        final String roundTrippedGremlin = ((Traversal<?, ?>) parsed).asAdmin().getGremlinLang().getGremlin();

        assertEquals("GremlinLang round-trip should preserve traversal structure for: " + originalGremlin,
                originalGremlin, roundTrippedGremlin);
    }

    @Test
    public void shouldRoundTripHasWithTraversalValue() {
        // g.V().has("name", __.values("x"))
        assertRoundTrip(g.V().has("name", __.values("x")));
    }

    @Test
    public void shouldRoundTripHasWithPredicateTraversal() {
        // g.V().has("name", P.eq(__.values("x")))
        assertRoundTrip(g.V().has("name", P.eq(__.values("x").asAdmin())));
    }

    @Test
    public void shouldRoundTripVWithTraversal() {
        // g.V().V(__.select("ids"))
        assertRoundTrip(g.V().V(__.select("ids")));
    }

    @Test
    public void shouldRoundTripPropertyWithTraversal() {
        // g.V().property("key", __.select("val"))
        assertRoundTrip(g.V().property("key", __.select("val")));
    }

    @Test
    public void shouldRoundTripHasWithConstantTraversal() {
        // g.V().has("name", __.constant("marko"))
        assertRoundTrip(g.V().has("name", __.constant("marko")));
    }

    @Test
    public void shouldRoundTripLiteralHasUnchanged() {
        // Backward compatibility: literal has() should still round-trip correctly
        assertRoundTrip(g.V().has("name", "marko"));
    }

    @Test
    public void shouldRoundTripLiteralPredicateUnchanged() {
        // Backward compatibility: literal P.eq() should still round-trip correctly
        assertRoundTrip(g.V().has("name", P.eq("marko")));
    }

    @Test
    public void shouldRoundTripHasWithPGtTraversal() {
        // g.V().has("age", P.gt(__.constant(30)))
        assertRoundTrip(g.V().has("age", P.gt(__.constant(30).asAdmin())));
    }

    @Test
    public void shouldRoundTripEWithTraversal() {
        // g.V().E(__.select("edgeIds"))
        assertRoundTrip(g.V().E(__.select("edgeIds")));
    }

    @Test
    public void shouldRoundTripPropertyWithMapTraversal() {
        // g.V().property(__.V().project("a").by("name")) — the Map-producing form
        assertRoundTrip(g.V().property(__.V().project("a").by("name")));
    }
}
