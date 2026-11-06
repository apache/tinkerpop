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
package org.apache.tinkerpop.gremlin.gql;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link QueryGraph} GQL MATCH parsing and graph construction.
 */
public class QueryGraphTest {

    // -------------------------------------------------------------------------
    // Node pattern parsing
    // -------------------------------------------------------------------------

    @Test
    public void testAnonymousNode() {
        final QueryGraph g = QueryGraph.parse("MATCH ()");
        assertEquals(1, g.getNodes().size());
        assertEquals(0, g.getEdges().size());
        final QueryVertex n = g.getNodes().get(0);
        assertNull(n.getVariable());
        assertNull(n.getLabel());
    }

    @Test
    public void testVariableOnlyNode() {
        final QueryGraph g = QueryGraph.parse("MATCH (n)");
        assertEquals(1, g.getNodes().size());
        final QueryVertex n = g.getNodes().get(0);
        assertEquals("n", n.getVariable());
        assertNull(n.getLabel());
    }

    @Test
    public void testLabelOnlyNode() {
        final QueryGraph g = QueryGraph.parse("MATCH (:Person)");
        assertEquals(1, g.getNodes().size());
        final QueryVertex n = g.getNodes().get(0);
        assertNull(n.getVariable());
        assertEquals("Person", n.getLabel());
    }

    @Test
    public void testVariableAndLabelNode() {
        final QueryGraph g = QueryGraph.parse("MATCH (n:Person)");
        assertEquals(1, g.getNodes().size());
        final QueryVertex n = g.getNodes().get(0);
        assertEquals("n", n.getVariable());
        assertEquals("Person", n.getLabel());
    }

    // -------------------------------------------------------------------------
    // Edge pattern parsing — directed OUT
    // -------------------------------------------------------------------------

    @Test
    public void testOutEdgeWithLabel() {
        final QueryGraph g = QueryGraph.parse("MATCH (a)-[:KNOWS]->(b)");
        assertEquals(2, g.getNodes().size());
        assertEquals(1, g.getEdges().size());
        final QueryEdge e = g.getEdges().get(0);
        assertNull(e.getVariable());
        assertEquals("KNOWS", e.getLabel());
        assertEquals(Direction.OUT, e.getDirection());
        assertEquals("a", e.getSource().getVariable());
        assertEquals("b", e.getTarget().getVariable());
    }

    @Test
    public void testOutEdgeWithVariableAndLabel() {
        final QueryGraph g = QueryGraph.parse("MATCH (a)-[r:KNOWS]->(b)");
        assertEquals(1, g.getEdges().size());
        final QueryEdge e = g.getEdges().get(0);
        assertEquals("r", e.getVariable());
        assertEquals("KNOWS", e.getLabel());
        assertEquals(Direction.OUT, e.getDirection());
    }

    @Test
    public void testOutEdgeVariableOnly() {
        final QueryGraph g = QueryGraph.parse("MATCH (a)-[r]->(b)");
        assertEquals(1, g.getEdges().size());
        final QueryEdge e = g.getEdges().get(0);
        assertEquals("r", e.getVariable());
        assertNull(e.getLabel());
        assertEquals(Direction.OUT, e.getDirection());
    }

    @Test
    public void testOutEdgeAnonymous() {
        final QueryGraph g = QueryGraph.parse("MATCH (a)-[]->(b)");
        assertEquals(1, g.getEdges().size());
        final QueryEdge e = g.getEdges().get(0);
        assertNull(e.getVariable());
        assertNull(e.getLabel());
        assertEquals(Direction.OUT, e.getDirection());
    }

    // -------------------------------------------------------------------------
    // Edge pattern parsing — directed IN
    // -------------------------------------------------------------------------

    @Test
    public void testInEdgeWithLabel() {
        final QueryGraph g = QueryGraph.parse("MATCH (a)<-[:KNOWS]-(b)");
        assertEquals(1, g.getEdges().size());
        final QueryEdge e = g.getEdges().get(0);
        assertNull(e.getVariable());
        assertEquals("KNOWS", e.getLabel());
        assertEquals(Direction.IN, e.getDirection());
        assertEquals("a", e.getSource().getVariable());
        assertEquals("b", e.getTarget().getVariable());
    }

    @Test
    public void testInEdgeWithVariableAndLabel() {
        final QueryGraph g = QueryGraph.parse("MATCH (a)<-[r:KNOWS]-(b)");
        assertEquals(1, g.getEdges().size());
        final QueryEdge e = g.getEdges().get(0);
        assertEquals("r", e.getVariable());
        assertEquals("KNOWS", e.getLabel());
        assertEquals(Direction.IN, e.getDirection());
    }

    // -------------------------------------------------------------------------
    // Edge pattern parsing — undirected (BOTH)
    // -------------------------------------------------------------------------

    @Test
    public void testUndirectedEdge() {
        final QueryGraph g = QueryGraph.parse("MATCH (a)-[:KNOWS]-(b)");
        assertEquals(1, g.getEdges().size());
        final QueryEdge e = g.getEdges().get(0);
        assertEquals("KNOWS", e.getLabel());
        assertEquals(Direction.BOTH, e.getDirection());
    }

    @Test
    public void testUndirectedEdgeAnonymous() {
        final QueryGraph g = QueryGraph.parse("MATCH (a)-[]-(b)");
        assertEquals(1, g.getEdges().size());
        assertEquals(Direction.BOTH, g.getEdges().get(0).getDirection());
    }

    // -------------------------------------------------------------------------
    // Multi-hop paths
    // -------------------------------------------------------------------------

    @Test
    public void testTwoHopPath() {
        final QueryGraph g = QueryGraph.parse("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)");
        assertEquals(3, g.getNodes().size());
        assertEquals(2, g.getEdges().size());

        final QueryEdge e1 = g.getEdges().get(0);
        assertEquals("KNOWS", e1.getLabel());
        assertEquals("a", e1.getSource().getVariable());
        assertEquals("b", e1.getTarget().getVariable());

        final QueryEdge e2 = g.getEdges().get(1);
        assertEquals("WORKS_AT", e2.getLabel());
        assertEquals("b", e2.getSource().getVariable());
        assertEquals("c", e2.getTarget().getVariable());
    }

    // -------------------------------------------------------------------------
    // Multiple comma-separated patterns
    // -------------------------------------------------------------------------

    @Test
    public void testMultiplePatterns() {
        final QueryGraph g = QueryGraph.parse("MATCH (a:Person)-[:KNOWS]->(b:Person), (b)-[:WORKS_AT]->(c:Company)");
        // 'b' is shared across patterns — should be 3 distinct nodes
        assertEquals(3, g.getNodes().size());
        assertEquals(2, g.getEdges().size());

        // Verify that 'b' is the same QueryVertex instance in both edges
        final QueryEdge e1 = g.getEdges().get(0);
        final QueryEdge e2 = g.getEdges().get(1);
        assertSame("Variable 'b' must resolve to the same QueryVertex", e1.getTarget(), e2.getSource());
    }

    @Test
    public void testMultiplePatternsDistinctVariables() {
        final QueryGraph g = QueryGraph.parse("MATCH (x)-[:A]->(y), (p)-[:B]->(q)");
        assertEquals(4, g.getNodes().size());
        assertEquals(2, g.getEdges().size());
    }

    // -------------------------------------------------------------------------
    // Variable identity (deduplication)
    // -------------------------------------------------------------------------

    @Test
    public void testVariableDeduplication() {
        final QueryGraph g = QueryGraph.parse("MATCH (n)-[:A]->(n)");
        // 'n' appears twice but should be the same node
        assertEquals(1, g.getNodes().size());
        final QueryEdge e = g.getEdges().get(0);
        assertSame(e.getSource(), e.getTarget());
    }

    // -------------------------------------------------------------------------
    // Case-insensitive MATCH keyword
    // -------------------------------------------------------------------------

    @Test
    public void testLowercaseMatch() {
        final QueryGraph g = QueryGraph.parse("match (a)-[:KNOWS]->(b)");
        assertEquals(1, g.getEdges().size());
        assertEquals(Direction.OUT, g.getEdges().get(0).getDirection());
    }

    @Test
    public void testMixedCaseMatch() {
        final QueryGraph g = QueryGraph.parse("Match (a:Person)");
        assertEquals(1, g.getNodes().size());
    }

    // -------------------------------------------------------------------------
    // Label and predicate merging across patterns
    // -------------------------------------------------------------------------

    @Test
    public void testNodePredicatesMergedAcrossPatterns() {
        final QueryGraph g = QueryGraph.parse(
                "MATCH (n {age: 29})-[:KNOWS]->(m), (n {name: 'marko'})");
        final QueryVertex n = g.getNodes().stream()
                .filter(v -> "n".equals(v.getVariable())).findFirst()
                .orElseThrow(AssertionError::new);
        assertEquals(2, n.getPredicates().size());
        assertTrue(n.getPredicates().stream().anyMatch(p -> "age".equals(p.getKey())));
        assertTrue(n.getPredicates().stream().anyMatch(p -> "name".equals(p.getKey())));
    }

    @Test
    public void testNodeLabelRefinedFromUnlabeledToLabeled() {
        final QueryGraph g = QueryGraph.parse("MATCH (n)-[:KNOWS]->(m), (n:Person)");
        assertEquals(2, g.getNodes().size());
        final QueryVertex n = g.getNodes().get(0);
        assertEquals("n", n.getVariable());
        assertEquals("Person", n.getLabel());
    }

    @Test
    public void testNodeLabelPreservedWhenSecondOccurrenceUnlabeled() {
        final QueryGraph g = QueryGraph.parse("MATCH (n:Person)-[:KNOWS]->(m), (n)");
        final QueryVertex n = g.getNodes().get(0);
        assertEquals("Person", n.getLabel());
    }

    // -------------------------------------------------------------------------
    // Parse failure
    // -------------------------------------------------------------------------

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidInputThrows() {
        QueryGraph.parse("SELECT * FROM foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingMatchKeywordThrows() {
        QueryGraph.parse("(a)-[:KNOWS]->(b)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConflictingLabelOnSameVariableThrows() {
        QueryGraph.parse("MATCH (n:Person)-[:KNOWS]->(n:Animal)");
    }

    @Test
    public void testSameVariableSameLabelIsAllowed() {
        final QueryGraph g = QueryGraph.parse("MATCH (n:Person)-[:KNOWS]->(n:Person)");
        assertEquals(1, g.getNodes().size());
        assertEquals("Person", g.getNodes().get(0).getLabel());
    }

    // -------------------------------------------------------------------------
    // Node / edge variable name conflicts
    // -------------------------------------------------------------------------

    @Test(expected = IllegalArgumentException.class)
    public void testEdgeVarConflictingWithNodeVarThrows() {
        // 'a' is introduced as a node variable first, then reused as an edge variable
        QueryGraph.parse("MATCH (a:Person)-[a:KNOWS]->(b:Person)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNodeVarConflictingWithEdgeVarThrows() {
        // 'e' is introduced as an edge variable first, then reused as a node variable
        QueryGraph.parse("MATCH (a:Person)-[e:KNOWS]->(e:Person)");
    }

    @Test
    public void testDistinctNodeAndEdgeVariablesAreAllowed() {
        final QueryGraph g = QueryGraph.parse("MATCH (a:Person)-[r:KNOWS]->(b:Person)");
        assertEquals(2, g.getNodes().size());
        assertEquals(1, g.getEdges().size());
        assertEquals("r", g.getEdges().get(0).getVariable());
    }

    // -------------------------------------------------------------------------
    // Malformed edge-arrow syntax: stray characters that were silently swallowed
    // by the lexer's default error recovery before the error listener was added.
    // -------------------------------------------------------------------------

    @Test
    public void parse_strayLeadingChar_throws() {
        assertParseError("MATCH (a:person)>-[:knows]->(b:person)");
    }

    @Test
    public void parse_strayLeadingCharUndirected_throws() {
        assertParseError("MATCH (a:person)>-[:knows]-(b:person)");
    }

    @Test
    public void parse_extraTrailingChar_throws() {
        assertParseError("MATCH (a:person)-[:knows]->>(b:person)");
    }

    @Test
    public void parse_garbageBothSides_throws() {
        assertParseError("MATCH (a:person)><<-[:knows]->><>(b:person)");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void assertParseError(final String gql) {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> QueryGraph.parse(gql));
        final String msg = ex.getMessage();
        assertTrue("message should reference the input",
                msg.contains("Failed to parse GQL MATCH expression"));
        assertTrue("message should include character position",
                msg.contains("character position at"));
    }
}
