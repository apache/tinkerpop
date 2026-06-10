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

import org.junit.Test;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class QueryGraphTest {

    // -------------------------------------------------------------------------
    // Malformed edge-arrow syntax: stray characters that were silently swallowed
    // by the lexer's default error recovery before the error listener was added.
    // -------------------------------------------------------------------------

    @Test
    public void parse_strayLeadingChar_throws() {
        // '>' before -[...]-> was being silently dropped, yielding a directed match
        assertParseError("MATCH (a:person)>-[:knows]->(b:person)");
    }

    @Test
    public void parse_strayLeadingCharUndirected_throws() {
        // '>' before -[...]- was being dropped, silently turning a bad pattern
        // into an undirected match that returned results in both directions
        assertParseError("MATCH (a:person)>-[:knows]-(b:person)");
    }

    @Test
    public void parse_extraTrailingChar_throws() {
        // '>>' after the bracket tail was being dropped, yielding a directed match
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
