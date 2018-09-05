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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.junit.Assert.assertEquals;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class AndTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_andXhasXage_gt_27X__outE_count_gte_2X_name();

    public abstract Traversal<Vertex, String> get_g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name();

    public abstract Traversal<Vertex, Vertex> get_g_V_asXaX_outXknowsX_and_outXcreatedX_inXcreatedX_asXaX_name();

    public abstract Traversal<Vertex, Vertex> get_g_V_asXaX_andXselectXaX_selectXaXX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_markoX_and_hasXname_markoX_and_hasXname_markoX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_andXhasXage_gt_27X__outE_count_gte_2X_name() {
        final Traversal<Vertex, String> traversal = get_g_V_andXhasXage_gt_27X__outE_count_gte_2X_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "josh"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh", "peter"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_outXknowsX_and_outXcreatedX_inXcreatedX_asXaX_name() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_asXaX_outXknowsX_and_outXcreatedX_inXcreatedX_asXaX_name();
        printTraversalForm(traversal);
        checkResults(Collections.singletonList(convertToVertex(graph, "marko")), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_andXselectXaX_selectXaXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_asXaX_andXselectXaX_selectXaXX();
        printTraversalForm(traversal);
        final List<Vertex> actual = traversal.toList();
        assertEquals(6, actual.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_markoX_and_hasXname_markoX_and_hasXname_markoX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_markoX_and_hasXname_markoX_and_hasXname_markoX();
        printTraversalForm(traversal);
        checkResults(Collections.singletonList(convertToVertex(graph, "marko")), traversal);
    }

    public static class Traversals extends AndTest {

        @Override
        public Traversal<Vertex, String> get_g_V_andXhasXage_gt_27X__outE_count_gte_2X_name() {
            return g.V().and(has("age", P.gt(27)), outE().count().is(P.gte(2l))).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name() {
            return g.V().and(outE(), has(T.label, "person").and().has("age", P.gte(32))).values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXaX_outXknowsX_and_outXcreatedX_inXcreatedX_asXaX_name() {
            return g.V().as("a").out("knows").and().out("created").in("created").as("a").values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXaX_andXselectXaX_selectXaXX() {
            return g.V().as("a").and(select("a"), select("a"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX_and_hasXname_markoX_and_hasXname_markoX() {
            return g.V().has("name", "marko").and().has("name", "marko").and().has("name", "marko");
        }
    }
}
