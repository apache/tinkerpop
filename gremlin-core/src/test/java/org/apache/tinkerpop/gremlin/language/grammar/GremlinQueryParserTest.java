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

import org.apache.tinkerpop.gremlin.process.traversal.GeoP;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.structure.geo.Geometry;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class GremlinQueryParserTest {
    private static final GraphTraversalSource g = EmptyGraph.instance().traversal();

    private static final Geometry SQUARE = Geometry.fromWkt("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))");

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

        assertEquals(g.V().has("name", P.gt((Object) null)).asAdmin().getGremlinLang(),
                t.asAdmin().getGremlinLang());
    }

    @Test
    public void shouldParseMultiVariablesWithNoOp() {
        final GremlinAntlrToJava gremlinAntlrToJava = new GremlinAntlrToJava("g",
                EmptyGraph.instance(), __::start, g,
                VariableResolver.NullVariableResolver.instance());
        final GraphTraversal<?, ?> t = (GraphTraversal<?, ?>) GremlinQueryParser.parse("g.V(a,b,c).has('name',gt(z))", gremlinAntlrToJava);

        assertEquals(g.V(null, null, null).has("name", P.gt((Object) null)).asAdmin().getGremlinLang(),
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

    @Test
    public void shouldParseGeoPredicateVariables() {
        final GremlinAntlrToJava gremlinAntlrToJava = new GremlinAntlrToJava("g",
                EmptyGraph.instance(), __::start, g,
                new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("xx1", SQUARE)));

        final GraphTraversal<?, ?> within = (GraphTraversal<?, ?>) GremlinQueryParser.parse(
                "g.V().has('loc',geoWithin(xx1))", gremlinAntlrToJava);
        assertEquals(g.V().has("loc", GeoP.geoWithin(GValue.of("xx1", SQUARE))).asAdmin().getGremlinLang(),
                within.asAdmin().getGremlinLang());
        assertEquals(Collections.singleton("xx1"), within.asAdmin().getGValueManager().getVariableNames());
    }

    @Test
    public void shouldParseGeoPredicateVariablesAsValues() {
        final GremlinAntlrToJava gremlinAntlrToJava = new GremlinAntlrToJava("g",
                EmptyGraph.instance(), __::start, g,
                new VariableResolver.DirectVariableResolver(ElementHelper.asMap("xx1", SQUARE)));

        final GraphTraversal<?, ?> t = (GraphTraversal<?, ?>) GremlinQueryParser.parse(
                "g.V().has('loc',geoWithin(xx1))", gremlinAntlrToJava);
        assertEquals(g.V().has("loc", GeoP.geoWithin(SQUARE)).asAdmin().getGremlinLang(),
                t.asAdmin().getGremlinLang());
        assertEquals(Collections.emptySet(), t.asAdmin().getGValueManager().getVariableNames());
    }

    @Test
    public void shouldNotParseAGeoPredicateArgumentThatCanNeverMatch() {
        final GremlinParserException gpe = assertThrows(GremlinParserException.class,
                () -> GremlinQueryParser.parse("g.V().has('loc',geoWithin(Geometry(\"POINT(1 2)\", 32618)))"));
        assertEquals("Invalid geo predicate: The geometry of geoWithin must be in the CARTESIAN or " +
                "GEOGRAPHIC model, but SRID 32618 is OTHER", gpe.getMessage());
    }

    @Test
    public void shouldNotParseAGeoContainsArgumentThatCanNeverMatch() {
        final GremlinParserException gpe = assertThrows(GremlinParserException.class,
                () -> GremlinQueryParser.parse("g.V().has('loc',geoContains(Geometry(\"POINT(1 2)\", 32618)))"));
        assertEquals("Invalid geo predicate: The geometry of geoContains must be in the CARTESIAN or " +
                "GEOGRAPHIC model, but SRID 32618 is OTHER", gpe.getMessage());
    }

    @Test
    public void shouldNotParseAGeoPredicateVariableThatIsNotAGeometry() {
        final GremlinAntlrToJava gremlinAntlrToJava = new GremlinAntlrToJava("g",
                EmptyGraph.instance(), __::start, g,
                new VariableResolver.DirectVariableResolver(ElementHelper.asMap("xx1", "POINT(1 2)")));

        final GremlinParserException gpe = assertThrows(GremlinParserException.class,
                () -> GremlinQueryParser.parse("g.V().has('loc',geoWithin(xx1))", gremlinAntlrToJava));
        assertEquals("Invalid geo predicate: The geometry of geoWithin must be a Geometry, but was String",
                gpe.getMessage());
    }
}
