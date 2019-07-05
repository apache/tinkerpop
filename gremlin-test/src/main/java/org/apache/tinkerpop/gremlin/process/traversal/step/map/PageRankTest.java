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

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class PageRankTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_pageRank_hasXpageRankX();

    public abstract Traversal<Vertex, Map<Object, List<Object>>> get_g_V_outXcreatedX_pageRank_withXedges_bothEX_withXpropertyName_projectRankXwithXtimes_0X_valueMapXname_projectRankX();

    public abstract Traversal<Vertex, String> get_g_V_pageRank_order_byXpageRank_descX_byXnameX_name();

    public abstract Traversal<Vertex, String> get_g_V_pageRank_order_byXpageRank_descX_name_limitX2X();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_pageRank_withXedges_outEXknowsXX_withXpropertyName_friendRankX_project_byXnameX_byXvaluesXfriendRankX_mathX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXpersonX_pageRank_withXpropertyName_pageRankX_project_byXnameX_byXvaluesXpageRankX_mathX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_pageRank_withXpropertyName_pageRankX_asXaX_outXknowsX_pageRank_asXbX_selectXa_bX_by_byXmathX();

    public abstract Traversal<Vertex, Map<Object, List<Object>>> get_g_V_hasLabelXsoftwareX_hasXname_rippleX_pageRankX1X_withXedges_inEXcreatedXX_withXtimes_1X_withXpropertyName_priorsX_inXcreatedX_unionXboth__identityX_valueMapXname_priorsX();

    public abstract Traversal<Vertex, Map<Object, List<Vertex>>> get_g_V_outXcreatedX_groupXmX_byXlabelX_pageRankX1X_withXpropertyName_pageRankXX_withXedges_inEXX_withXtimes_1X_inXcreatedX_groupXmX_byXpageRankX_capXmX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_pageRank_hasXpageRankX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_pageRank_hasXpageRankX();
        printTraversalForm(traversal);
        assertEquals(6, IteratorUtils.count(traversal));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_pageRank_withXedges_bothEX_withXpropertyName_projectRankXwithXtimes_0X_valueMapXname_projectRankX() {
        final Traversal<Vertex, Map<Object, List<Object>>> traversal = get_g_V_outXcreatedX_pageRank_withXedges_bothEX_withXpropertyName_projectRankXwithXtimes_0X_valueMapXname_projectRankX();
        printTraversalForm(traversal);
        final List<Map<Object, List<Object>>> result = traversal.toList();
        assertEquals(4, result.size());
        final Map<String, Double> map = new HashMap<>();
        result.forEach(m -> map.put((String) m.get("name").get(0), (Double) m.get("projectRank").get(0)));
        assertEquals(2, map.size());
        assertTrue(map.containsKey("lop"));
        assertTrue(map.containsKey("ripple"));
        assertTrue(map.get("lop") > map.get("ripple"));
        assertEquals(3.0d, map.get("lop"), 0.001d);
        assertEquals(1.0d, map.get("ripple"), 0.001d);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_pageRank_order_byXpageRank_descX_byXnameX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_pageRank_order_byXpageRank_descX_byXnameX_name();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(6, names.size());
        assertEquals("lop", names.get(0));
        assertEquals("ripple", names.get(1));
        assertEquals("josh", names.get(2));
        assertEquals("vadas", names.get(3));
        assertEquals("marko", names.get(4));
        assertEquals("peter", names.get(5));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_pageRank_withXedges_outEXknowsXX_withXpropertyName_friendRankX_project_byXnameX_byXvaluesXfriendRankX_mathX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_pageRank_withXedges_outEXknowsXX_withXpropertyName_friendRankX_project_byXnameX_byXvaluesXfriendRankX_mathX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            final String name = (String) map.get("name");
            final Double friendRank = (Double) map.get("friendRank");
            if (name.equals("lop") || name.equals("ripple") || name.equals("peter") || name.equals("marko"))
                assertEquals(15.0, friendRank, 0.01);
            else
                assertEquals(21.0, friendRank, 0.01);

            counter++;
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_pageRank_order_byXpageRank_descX_name_limitX2X() {
        final Traversal<Vertex, String> traversal = get_g_V_pageRank_order_byXpageRank_descX_name_limitX2X();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(2, names.size());
        assertEquals("lop", names.get(0));
        assertEquals("ripple", names.get(1));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_pageRank_withXpropertyName_pageRankX_project_byXnameX_byXvaluesXpageRankX_mathX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasLabelXpersonX_pageRank_withXpropertyName_pageRankX_project_byXnameX_byXvaluesXpageRankX_mathX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            final String name = (String) map.get("name");
            final Double pageRank = (Double) map.get("pageRank");
            if (name.equals("marko") || name.equals("peter"))
                assertEquals(46.0, pageRank, 0.01);
            else
                assertEquals(59.0, pageRank, 0.01);

            counter++;
        }
        assertEquals(4, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_pageRank_withXpropertyName_pageRankX_asXaX_outXknowsX_pageRank_asXbX_selectXa_bX_by_byXmathX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_pageRank_withXpropertyName_pageRankX_asXaX_outXknowsX_pageRank_asXbX_selectXa_bX_by_byXmathX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            Vertex vertex = (Vertex) map.get("a");
            double pageRank = (Double) map.get("b");
            assertEquals(convertToVertexId("marko"), vertex.id());
            assertTrue(pageRank > 0.10d);
            counter++;
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXsoftwareX_hasXname_rippleX_pageRankX1X_withXedges_inEXcreatedXX_withXtimes_1X_withXpropertyName_priorsX_inXcreatedX_unionXboth__identityX_valueMapXname_priorsX() {
        final Traversal<Vertex, Map<Object, List<Object>>> traversal = get_g_V_hasLabelXsoftwareX_hasXname_rippleX_pageRankX1X_withXedges_inEXcreatedXX_withXtimes_1X_withXpropertyName_priorsX_inXcreatedX_unionXboth__identityX_valueMapXname_priorsX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Map<Object, List<Object>> map = traversal.next();
            assertEquals(2, map.size());
            String name = (String) map.get("name").get(0);
            double pageRank = (Double) map.get("priors").get(0);
            assertEquals(name.equals("josh") ? 1.0d : 0.0d, pageRank, 0.0001d);
            if (name.equals("peter") || name.equals("vadas"))
                fail("Peter or Vadas should not have been accessed");
            counter++;
        }
        assertEquals(4, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_groupXmX_byXlabelX_pageRankX1X_withXPROPERTY_NAME_pageRankXX_withXEDGES_inEXX_withXTIMES_1X_inXcreatedX_groupXmX_byXpageRankX_capXmX() {
        // [{2.0=[v[4], v[4], v[4], v[4]], 1.0=[v[6], v[6], v[6], v[1], v[1], v[1]], software=[v[5], v[3], v[3], v[3]]}]
        final Traversal<Vertex, Map<Object, List<Vertex>>> traversal = get_g_V_outXcreatedX_groupXmX_byXlabelX_pageRankX1X_withXpropertyName_pageRankXX_withXedges_inEXX_withXtimes_1X_inXcreatedX_groupXmX_byXpageRankX_capXmX();
        printTraversalForm(traversal);
        final Map<Object, List<Vertex>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(3, map.size());
        assertTrue(map.containsKey("software"));
        map.forEach((k, v) -> {
            boolean found = false;
            if (!k.equals("software") && v.size() == 4) {
                assertEquals(2.0d, ((Number) k).doubleValue(), 0.01d);
                assertEquals(4, v.stream().filter(vertex -> vertex.id().equals(convertToVertexId(graph, "josh"))).count());
                found = true;
            } else if (v.size() == 6) {
                assertEquals(1.0d, ((Number) k).doubleValue(), 0.01d);
                assertEquals(3, v.stream().filter(vertex -> vertex.id().equals(convertToVertexId(graph, "peter"))).count());
                assertEquals(3, v.stream().filter(vertex -> vertex.id().equals(convertToVertexId(graph, "marko"))).count());
                found = true;
            } else if (v.size() == 4) {
                assertEquals("software", k);
                assertEquals(3, v.stream().filter(vertex -> vertex.id().equals(convertToVertexId(graph, "lop"))).count());
                assertEquals(1, v.stream().filter(vertex -> vertex.id().equals(convertToVertexId(graph, "ripple"))).count());
                found = true;
            }

            if (!found)
                fail("There are too many key/values: " + k + "--" + v);
        });
    }

    public static class Traversals extends PageRankTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_pageRank_hasXpageRankX() {
            return g.V().pageRank().has(PageRankVertexProgram.PAGE_RANK);
        }

        @Override
        public Traversal<Vertex, Map<Object, List<Object>>> get_g_V_outXcreatedX_pageRank_withXedges_bothEX_withXpropertyName_projectRankXwithXtimes_0X_valueMapXname_projectRankX() {
            return g.V().out("created").pageRank().with(PageRank.edges, __.bothE()).with(PageRank.propertyName, "projectRank").with(PageRank.times, 0).valueMap("name", "projectRank");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_pageRank_withXedges_outEXknowsXX_withXpropertyName_friendRankX_project_byXnameX_byXvaluesXfriendRankX_mathX() {
            return g.V().pageRank().with(PageRank.edges, __.outE("knows")).with(PageRank.propertyName, "friendRank").project("name", "friendRank").by("name").by(__.values("friendRank").math("ceil(_ * 100)"));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_pageRank_order_byXpageRank_descX_byXnameX_name() {
            return g.V().pageRank().order().by(PageRankVertexProgram.PAGE_RANK, Order.desc).by("name").values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_pageRank_order_byXpageRank_descX_name_limitX2X() {
            return g.V().pageRank().order().by(PageRankVertexProgram.PAGE_RANK, Order.desc).<String>values("name").limit(2);
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXpersonX_pageRank_withXpropertyName_pageRankX_project_byXnameX_byXvaluesXpageRankX_mathX() {
            return g.V().hasLabel("person").pageRank().with(PageRank.propertyName, "pageRank").project("name", "pageRank").by("name").by(__.values("pageRank").math("ceil(_ * 100)"));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_pageRank_withXpropertyName_pageRankX_asXaX_outXknowsX_pageRank_asXbX_selectXa_bX_by_byXmathX() {
            return g.V().pageRank().with(PageRank.propertyName, "pageRank").as("a").out("knows").values("pageRank").as("b").select("a", "b").by().by(__.math("ceil(_ * 100)"));
        }

        @Override
        public Traversal<Vertex, Map<Object, List<Object>>> get_g_V_hasLabelXsoftwareX_hasXname_rippleX_pageRankX1X_withXedges_inEXcreatedXX_withXtimes_1X_withXpropertyName_priorsX_inXcreatedX_unionXboth__identityX_valueMapXname_priorsX() {
            return g.V().hasLabel("software").has("name", "ripple").pageRank(1.0).with(PageRank.edges, __.inE("created")).with(PageRank.times, 1).with(PageRank.propertyName, "priors").in("created").union(__.both(), __.identity()).valueMap("name", "priors");
        }

        @Override
        public Traversal<Vertex, Map<Object, List<Vertex>>> get_g_V_outXcreatedX_groupXmX_byXlabelX_pageRankX1X_withXpropertyName_pageRankXX_withXedges_inEXX_withXtimes_1X_inXcreatedX_groupXmX_byXpageRankX_capXmX() {
            return g.V().out("created").group("m").by(T.label).pageRank(1.0).with(PageRank.propertyName, "pageRank").with(PageRank.edges, __.inE()).with(PageRank.times, 1).in("created").group("m").by("pageRank").cap("m");
        }
    }
}
