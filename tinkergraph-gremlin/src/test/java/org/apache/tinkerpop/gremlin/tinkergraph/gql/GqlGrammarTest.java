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
package org.apache.tinkerpop.gremlin.tinkergraph.gql;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertThrows;

/**
 * Validates that {@link GQLParser} correctly parses the minimal GQL MATCH patterns
 * defined by the grammar in {@code GQL.g4}.
 */
@RunWith(Parameterized.class)
public class GqlGrammarTest {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> patterns() {
        return Arrays.asList(new Object[][]{
                // ── Node patterns ─────────────────────────────────────────────────────
                // Fully anonymous node
                {"MATCH ()"},
                // Variable-only node
                {"MATCH (n)"},
                // Label-only node
                {"MATCH (:Person)"},
                // Variable and label node
                {"MATCH (n:Person)"},

                // ── Directed edges -[...]->, node labels on both sides ────────────────
                // Label only on edge
                {"MATCH (n)-[:KNOWS]->(m)"},
                // Variable + label on edge
                {"MATCH (n)-[e:KNOWS]->(m)"},
                // Variable only on edge
                {"MATCH (n)-[e]->(m)"},
                // Anonymous edge
                {"MATCH (n)-[]->(m)"},
                // Node labels with directed edge
                {"MATCH (n:Person)-[:KNOWS]->(m:Person)"},

                // ── Reverse directed edges <-[...]- ───────────────────────────────────
                // Label only on edge
                {"MATCH (n)<-[:KNOWS]-(m)"},
                // Variable + label on edge
                {"MATCH (n)<-[e:KNOWS]-(m)"},
                // Variable only on edge
                {"MATCH (n)<-[e]-(m)"},
                // Anonymous edge
                {"MATCH (n)<-[]-(m)"},
                // Node labels with reverse directed edge
                {"MATCH (n:Person)<-[:KNOWS]-(m:Person)"},

                // ── Undirected edges -[...]- ──────────────────────────────────────────
                // Label only on edge
                {"MATCH (n)-[:KNOWS]-(m)"},
                // Variable + label on edge
                {"MATCH (n)-[e:KNOWS]-(m)"},
                // Variable only on edge
                {"MATCH (n)-[e]-(m)"},
                // Anonymous edge
                {"MATCH (n)-[]-(m)"},
                // Node labels with undirected edge
                {"MATCH (n:Person)-[:KNOWS]-(m:Person)"},

                // ── Multiple comma-separated path patterns ────────────────────────────
                {"MATCH (n:Person), (m:Movie)"},
                {"MATCH (n:Person)-[:ACTED_IN]->(m:Movie), (m)-[:IN_GENRE]->(g:Genre)"},
                {"MATCH (n)-[:KNOWS]->(m), (m)<-[:LIKES]-(p)"},
                {"MATCH (), (:Label), (v:Label)"},

                // ── Chained path patterns (multiple edges) ────────────────────────────
                {"MATCH (a)-[:KNOWS]->(b)-[:LIKES]->(c)"},
                {"MATCH (a:Person)-[e1:KNOWS]->(b:Person)-[e2:LIKES]-(c:Thing)"},
                {"MATCH (a)<-[:X]-(b)-[:Y]->(c)"},

                // ── Case-insensitive MATCH keyword ────────────────────────────────────
                {"match (n:Person)"},
                {"Match (n:Actor)-[r:ACTED_IN]->(f:Film)"},
                {"MATCH (n)-[:KNOWS]->(m:Person)"},
                {"mAtCh ()-[]-()"},
        });
    }

    private final String input;

    public GqlGrammarTest(final String input) {
        this.input = input;
    }

    @Test
    public void shouldParseValidPattern() {
        try {
            parse(input);
        } catch (final ParseCancellationException ex) {
            fail("Failed to parse [" + input + "]: " + ex.getMessage());
        }
    }

    @Test
    public void shouldRejectMissingMatchKeyword() {
        assertThrows(ParseCancellationException.class, () -> parse("(n)-[:KNOWS]->(m)"));
    }

    @Test
    public void shouldRejectAbbreviatedDirectedEdge() {
        // -->, <--, -- abbreviations are out of scope; only bracket form is supported
        assertThrows(ParseCancellationException.class, () -> parse("MATCH (n)-->(m)"));
    }

    @Test
    public void shouldRejectAbbreviatedUndirectedEdge() {
        assertThrows(ParseCancellationException.class, () -> parse("MATCH (n)--(m)"));
    }

    @Test
    public void shouldRejectPropertyFilter() {
        // Property maps {key: value} are explicitly out of scope
        assertThrows(ParseCancellationException.class, () -> parse("MATCH (n {name: 'Alice'})"));
    }

    @Test
    public void shouldRejectWhereClause() {
        // WHERE is not part of this minimal grammar
        assertThrows(ParseCancellationException.class, () -> parse("MATCH (n:Person) WHERE n.age > 30"));
    }

    @Test
    public void shouldRejectUnbalancedParentheses() {
        assertThrows(ParseCancellationException.class, () -> parse("MATCH (n"));
    }

    private void parse(final String input) {
        final GQLLexer lexer = new GQLLexer(CharStreams.fromString(input));
        lexer.removeErrorListeners();
        lexer.addErrorListener(ThrowingErrorListener.INSTANCE);

        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        final GQLParser parser = new GQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(ThrowingErrorListener.INSTANCE);
        parser.setErrorHandler(new BailErrorStrategy());

        parser.matchClause();
    }

    /**
     * Error listener that converts ANTLR syntax errors into exceptions so that
     * test failures contain the error message rather than being swallowed.
     */
    private static final class ThrowingErrorListener extends BaseErrorListener {

        static final ThrowingErrorListener INSTANCE = new ThrowingErrorListener();

        @Override
        public void syntaxError(final Recognizer<?, ?> recognizer,
                                final Object offendingSymbol,
                                final int line,
                                final int charPositionInLine,
                                final String msg,
                                final RecognitionException ex) {
            throw new ParseCancellationException(
                    "line " + line + ":" + charPositionInLine + " " + msg);
        }
    }
}
