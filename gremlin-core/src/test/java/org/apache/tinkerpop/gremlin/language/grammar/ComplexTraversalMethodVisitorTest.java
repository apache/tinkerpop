/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.where;
import static org.junit.Assert.assertEquals;

@RunWith(Enclosed.class)
public class ComplexTraversalMethodVisitorTest {

    private static GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());

    /**
     *  Test multiple level of nested graph traversal step
     */
    @RunWith(Parameterized.class)
    public static class NestedTest {

        @Parameterized.Parameter(value = 0)
        public String script;

        @Parameterized.Parameter(value = 1)
        public GraphTraversal expected;

        @Parameterized.Parameters()
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"g.V().where(both())", g.V().where(both())},
                    {"g.V().where(where(both()))", g.V().where(where(both()))},
                    {"g.V().where(where(where(both())))", g.V().where(where(where(both())))},
                    {"g.V().where(where(where(where(both()))))", g.V().where(where(where(where(both()))))},
                    {"g.V().where(__.where(both()))", g.V().where(__.where(both()))},
                    {"g.V().where(__.where(where(both())))", g.V().where(__.where(where(both())))},
                    {"g.V().where(__.where(where(__.where(both()))))", g.V().where(__.where(where(__.where(both()))))},
                    {"g.V().where(__.where(where(__.where(where(both())))))", g.V().where(__.where(where(__.where(where(both())))))},
                    {"g.V().where(__.where(__.where(__.where(where(both())))))", g.V().where(__.where(__.where(__.where(where(both())))))},
            });
        }

        @Test
        public void testNestedGraphTraversal() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.QueryContext ctx = parser.query();
            final Object result = new GremlinAntlrToJava().visitQuery(ctx);

            assertEquals(expected.toString(), result.toString());
        }
    }

    /**
     *  Test multiple chains of chained graph traversal step
     */
    @RunWith(Parameterized.class)
    public static class ChainedTest {

        @Parameterized.Parameter(value = 0)
        public String script;

        @Parameterized.Parameter(value = 1)
        public GraphTraversal expected;

        @Parameterized.Parameters()
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"g.V().out()", g.V().out()},
                    {"g.V().out().out()", g.V().out().out()},
                    {"g.V().out().out().out()", g.V().out().out().out()},
                    {"g.V().out().out().out().out()", g.V().out().out().out().out()}
            });
        }

        @Test
        public void testChainedGraphTraversal() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.QueryContext ctx = parser.query();
            final Object result = new GremlinAntlrToJava().visitQuery(ctx);

            assertEquals(expected.toString(), result.toString());
        }
    }

    /**
     * Test multiple terminated graph traversals
     */
    @RunWith(Parameterized.class)
    @Ignore("do we want to let gremlin-language handle terminators like this in a single script or are terminators external to the language?")
    public static class TerminatedTest {

        @Parameterized.Parameter(value = 0)
        public String script;

        @Parameterized.Parameter(value = 1)
        public GraphTraversal expected;

        @Parameterized.Parameters()
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"g.V().values('a').toSet()", g.V().values("a").toSet()},
                    {"g.V().addV().property('d', g.V().values('a').count().next())", g.V().addV().property("d", g.V().values("a").count().next())},
                    {"g.V().where(__.values('a').is(within(g.V().values('a').toSet())))", g.V().where(__.values("a").is(P.within(g.V().values("a").toSet())))}
            });
        }

        @Test
        public void testTerminatedGraphTraversal() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.QueryContext ctx = parser.query();
            final Object result = new GremlinAntlrToJava().visitQuery(ctx);

            assertEquals(expected.toString(), result.toString());
        }
    }
}

