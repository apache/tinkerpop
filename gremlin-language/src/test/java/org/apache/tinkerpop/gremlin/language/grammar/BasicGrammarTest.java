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

import org.junit.Test;

/**
 * Basic smoke testing of the parser.
 */
public class BasicGrammarTest extends AbstractGrammarTest {
    @Test
    public void shouldParseV() {
        parse("g.V()", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseHasLabelP() {
        parse("g.V().hasLabel(eq(\"person\"))", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseUntilPredicate() {
        parse("g.V().repeat(__.out()).until(gt(2))", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseEmitPredicate() {
        parse("g.V().repeat(__.out()).emit(gt(2))", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseFilterPredicate() {
        parse("g.V().values(\"age\").filter(gt(2))", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseOptionTraversal() {
        parse("g.V().branch(__.values(\"name\")).option(__.identity())", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseBarrierInt() {
        parse("g.V().barrier(2500)", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseProfileString() {
        parse("g.V().profile(\"gremlin.metrics\")", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseToV() {
        parse("g.E().toV(Direction.OUT)", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseMatchStringMap() {
        parse("g.V().match(\"gremlin\", [:])", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseEmptyQuery() {
        parse("\"\"", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseTerminatedTraversal() {
        parse("g.inject(g.V().count().next())", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParsePdtLiteral() {
        parse("g.inject(PDT(\"x\", \"y\"))", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseWithOptionsAll() {
        parse("g.V().valueMap().with(WithOptions.tokens, WithOptions.all)", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseWithOptionsKeys() {
        parse("g.V().valueMap().with(WithOptions.tokens, WithOptions.keys)", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseWithOptionsNone() {
        parse("g.V().valueMap().with(WithOptions.tokens, WithOptions.none)", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParsePageRankTimes() {
        parse("g.V().pageRank().with(PageRank.times, 10)", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParsePeerPressureEdges() {
        parse("g.V().peerPressure().with(PeerPressure.edges, __.outE())", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParsePeerPressureTimes() {
        parse("g.V().peerPressure().with(PeerPressure.times, 10)", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseShortestPathMaxDistance() {
        parse("g.V().shortestPath().with(ShortestPath.maxDistance, 5)", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseConnectedComponentComponent() {
        parse("g.V().connectedComponent().with(ConnectedComponent.component, \"cc\")", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseTraversalSourceWithString() {
        parse("g.with(\"x\")", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseTraversalSourceWithStringObject() {
        parse("g.with(\"x\", 123)", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseTypeOfBareGTypeForms() {
        final String[] bareGTypes = new String[]{
                "bigDecimal", "BIGDECIMAL", "bigInt", "BIGINT", "binary", "BINARY", "boolean", "BOOLEAN",
                "byte", "BYTE", "char", "CHAR", "datetime", "DATETIME", "double", "DOUBLE", "duration", "DURATION",
                "edge", "EDGE", "float", "FLOAT", "graph", "GRAPH", "int", "INT", "list", "LIST", "long", "LONG",
                "map", "MAP", "null", "NULL", "number", "NUMBER", "path", "PATH", "property", "PROPERTY",
                "set", "SET", "short", "SHORT", "string", "STRING", "tree", "TREE", "UUID", "uuid",
                "vertex", "VERTEX", "vproperty", "VPROPERTY"
        };
        for (final String t : bareGTypes) {
            parse(String.format("g.V().is(typeOf(%s))", t), ParserRule.QUERY_LIST);
        }
    }

    @Test
    public void shouldParseTransactionBegin() {
        parse("g.tx().begin()", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseTransactionCommit() {
        parse("g.tx().commit()", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseTransactionRollback() {
        parse("g.tx().rollback()", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseQueryToString() {
        parse("g.V().toString()", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseWithinWithoutForms() {
        parse("g.V().is(within())", ParserRule.QUERY_LIST);
        parse("g.V().is(without())", ParserRule.QUERY_LIST);
        parse("g.V().is(within(1, 2, 3))", ParserRule.QUERY_LIST);
        parse("g.V().is(without(1, 2, 3))", ParserRule.QUERY_LIST);
    }

    @Test
    public void shouldParseTypeOfString() {
        parse("g.V().is(typeOf(\"int\"))", ParserRule.QUERY_LIST);
    }
}
