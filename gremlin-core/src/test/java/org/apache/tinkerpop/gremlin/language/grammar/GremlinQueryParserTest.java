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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GremlinQueryParserTest {
    private static final GraphTraversalSource g = EmptyGraph.instance().traversal();

    @Test
    public void shouldParseEmpty() {
        assertEquals("", GremlinQueryParser.parse("\"\""));
        assertEquals("", GremlinQueryParser.parse("''"));
    }

    @Test
    public void shouldParseVariables() {
        final GremlinAntlrToJava gremlinAntlrToJava = new GremlinAntlrToJava("g",
                EmptyGraph.instance(), __::start, g,
                new VariableResolver.DirectVariableResolver(ElementHelper.asMap("z", 50)));
        final GraphTraversal<?, ?> t = (GraphTraversal<?, ?>) GremlinQueryParser.parse("g.V().has('name',gt(z))", gremlinAntlrToJava);

        assertEquals(g.V().has("name", P.gt(50)).asAdmin().getGremlinLang(),
                t.asAdmin().getGremlinLang());
    }

    @Test
    public void shouldParseVariablesInVarargs() {
        final GremlinAntlrToJava gremlinAntlrToJava = new GremlinAntlrToJava("g",
                EmptyGraph.instance(), __::start, g,
                new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", 100,
                                                                                 "y", 200,
                                                                                 "z", 50)));

        GraphTraversal<?, ?> t = (GraphTraversal<?, ?>) GremlinQueryParser.parse("g.V().has('name',gt(z))", gremlinAntlrToJava);
        assertEquals(g.V().has("name", P.gt(50)).asAdmin().getGremlinLang(),
                t.asAdmin().getGremlinLang());

        t = (GraphTraversal<?, ?>) GremlinQueryParser.parse("g.V(x).has('name',gt(z))", gremlinAntlrToJava);
        assertEquals(g.V(100).has("name", P.gt(50)).asAdmin().getGremlinLang(),
                t.asAdmin().getGremlinLang());

        t = (GraphTraversal<?, ?>) GremlinQueryParser.parse("g.V(x, y, 300).has('name',gt(z))", gremlinAntlrToJava);
        assertEquals(g.V(100, 200, 300).has("name", P.gt(50)).asAdmin().getGremlinLang(),
                t.asAdmin().getGremlinLang());
    }

    @Test(expected = GremlinParserException.class)
    public void shouldNotParseVariablesInList() {
        final GremlinAntlrToJava gremlinAntlrToJava = new GremlinAntlrToJava("g",
                EmptyGraph.instance(), __::start, g,
                new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", 100,
                        "y", 200,
                        "z", 50)));
        GremlinQueryParser.parse("g.V([x, y, 300]).has('name',gt(z))", gremlinAntlrToJava);
    }

    @Test(expected = GremlinParserException.class)
    public void shouldNotParseVariablesWhichAreTraversalBased() {
        final GremlinAntlrToJava gremlinAntlrToJava = new GremlinAntlrToJava("g",
                EmptyGraph.instance(), __::start, g,
                new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", 100,
                        "y", 200,
                        "z", __.out())));
        GremlinQueryParser.parse("g.V([x, y, 300]).where(z)", gremlinAntlrToJava);
    }

    @Test
    public void shouldParseVariableWithNoOp() {
        final GremlinAntlrToJava gremlinAntlrToJava = new GremlinAntlrToJava("g",
                EmptyGraph.instance(), __::start, g,
                VariableResolver.NullVariableResolver.instance());
        final GraphTraversal<?, ?> t = (GraphTraversal<?, ?>) GremlinQueryParser.parse("g.V().has('name',gt(z))", gremlinAntlrToJava);

        assertEquals(g.V().has("name", P.gt(null)).asAdmin().getGremlinLang(),
                t.asAdmin().getGremlinLang());
    }

    @Test
    public void shouldParseMultiVariablesWithNoOp() {
        final GremlinAntlrToJava gremlinAntlrToJava = new GremlinAntlrToJava("g",
                EmptyGraph.instance(), __::start, g,
                VariableResolver.NullVariableResolver.instance());
        final GraphTraversal<?, ?> t = (GraphTraversal<?, ?>) GremlinQueryParser.parse("g.V(a,b,c).has('name',gt(z))", gremlinAntlrToJava);

        assertEquals(g.V(null, null, null).has("name", P.gt(null)).asAdmin().getGremlinLang(),
                t.asAdmin().getGremlinLang());
    }

    @Test(expected = GremlinParserException.class)
    public void shouldNotParseChildTraversalsSpawnedFromG() {
        final GremlinAntlrToJava gremlinAntlrToJava = new GremlinAntlrToJava("g",
                EmptyGraph.instance(), __::start, g, VariableResolver.NoVariableResolver.instance());
        try {
            GremlinQueryParser.parse("g.addE('knows').from(g.V(1))", gremlinAntlrToJava);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
}
