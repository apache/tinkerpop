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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class GraphTraversalSourceVisitorTest {

    private static final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());
    private GremlinAntlrToJava antlrToLanguage;

    @Parameterized.Parameter(value = 0)
    public String script;

    @Parameterized.Parameter(value = 1)
    public GraphTraversalSource traversalSource;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {
        return Arrays.asList(new Object[][]{
                {"g", g},
                {"g.withBulk(false)", g.withBulk(false)},
                {"g.withBulk(false).withPath()", g.withBulk(false).withPath()},
        });
    }

    @Before
    public void setup() throws Exception {
        antlrToLanguage = new GremlinAntlrToJava();
    }

    /**
     *  Test Graph traversal source visitor
     */
    @Test
    public void shouldHandleGraphTraversalSourceVisitor() {
        final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
        final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
        final GremlinParser.TraversalSourceContext ctx = parser.traversalSource();
        final GraphTraversalSource result = new GraphTraversalSourceVisitor(antlrToLanguage).visitTraversalSource(ctx);

        assertEquals(traversalSource.getGremlinLang(), result.getGremlinLang());
    }
}
