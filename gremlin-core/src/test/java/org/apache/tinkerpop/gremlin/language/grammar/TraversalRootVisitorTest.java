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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TraversalRootVisitorTest {
    private GraphTraversalSource g;
    private GremlinAntlrToJava antlrToLanguage;

    @Before
    public void setup() {
        g = new GraphTraversalSource(EmptyGraph.instance());
        antlrToLanguage = new GremlinAntlrToJava();
    }

    @Test
    public void testTraversalMethod_none()  {
        compare(g.V().none(), eval("g.V().none()"));
        compare(g.V().union(__.identity().none()), eval("g.V().union(__.identity().none())"));
    }

    private void compare(Object expected, Object actual) {
        assertEquals(((DefaultGraphTraversal) expected).asAdmin().getBytecode(),
                ((DefaultGraphTraversal) actual).asAdmin().getBytecode());
    }

    private Object eval(String query) {
        final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(query));
        final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
        return antlrToLanguage.visit(parser.queryList());
    }
}
