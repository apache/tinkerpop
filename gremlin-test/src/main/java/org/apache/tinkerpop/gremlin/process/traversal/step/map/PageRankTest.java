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
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

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

    public abstract Traversal<Vertex, Vertex> get_g_V_pageRank();

    public abstract Traversal<Vertex, String> get_g_V_pageRank_order_byXpageRank_decrX_name();

    public abstract Traversal<Vertex, String> get_g_V_pageRank_order_byXpageRank_decrX_name_limitX2X();

    public abstract Traversal<Vertex, Map<String, List<Object>>> get_g_V_pageRank_byXoutEXknowsXX_byXfriendRankX_valueMapXname_friendRankX();

    public abstract Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasLabelXpersonX_pageRank_byXpageRankX_order_byXpageRankX_valueMapXname_pageRankX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_pageRank_byXpageRankX_asXaX_outXknowsX_pageRank_asXbX_selectXa_bX();

    public abstract Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasLabelXsoftwareX_hasXname_rippleX_pageRankX1X_byXinEXcreatedXX_timesX1X_byXpriorsX_inXcreatedX_unionXboth__identityX_valueMapXname_priorsX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_pageRank() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_pageRank();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Vertex vertex = traversal.next();
            counter++;
            assertTrue(vertex.property(PageRankVertexProgram.PAGE_RANK).isPresent());
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_pageRank_order_byXpageRank_decrX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_pageRank_order_byXpageRank_decrX_name();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(6, names.size());
        assertEquals("lop", names.get(0));
        assertEquals("ripple", names.get(1));
        assertTrue(names.get(2).equals("josh") || names.get(2).equals("vadas"));
        assertTrue(names.get(3).equals("josh") || names.get(3).equals("vadas"));
        assertTrue(names.get(4).equals("marko") || names.get(4).equals("peter"));
        assertTrue(names.get(5).equals("marko") || names.get(5).equals("peter"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_pageRank_byXoutEXknowsXX_byXfriendRankX_valueMapXname_friendRankX() {
        final Traversal<Vertex, Map<String, List<Object>>> traversal = get_g_V_pageRank_byXoutEXknowsXX_byXfriendRankX_valueMapXname_friendRankX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Map<String, List<Object>> map = traversal.next();
            assertEquals(2, map.size());
            assertEquals(1, map.get("name").size());
            assertEquals(1, map.get("friendRank").size());
            String name = (String) map.get("name").get(0);
            Double friendRank = (Double) map.get("friendRank").get(0);
            if (name.equals("lop") || name.equals("ripple") || name.equals("peter") || name.equals("marko"))
                assertEquals(0.15, friendRank, 0.01);
            else
                assertEquals(0.21375, friendRank, 0.01);

            counter++;
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_pageRank_order_byXpageRank_decrX_name_limitX2X() {
        final Traversal<Vertex, String> traversal = get_g_V_pageRank_order_byXpageRank_decrX_name_limitX2X();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(2, names.size());
        assertEquals("lop", names.get(0));
        assertEquals("ripple", names.get(1));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_pageRank_byXpageRankX_order_byXpageRankX_valueMapXname_pageRankX() {
        final Traversal<Vertex, Map<String, List<Object>>> traversal = get_g_V_hasLabelXpersonX_pageRank_byXpageRankX_order_byXpageRankX_valueMapXname_pageRankX();
        printTraversalForm(traversal);
        int counter = 0;
        double lastPageRank = Double.MIN_VALUE;
        while (traversal.hasNext()) {
            final Map<String, List<Object>> map = traversal.next();
            assertEquals(2, map.size());
            assertEquals(1, map.get("name").size());
            assertEquals(1, map.get("pageRank").size());
            String name = (String) map.get("name").get(0);
            double pageRank = (Double) map.get("pageRank").get(0);
            assertTrue(pageRank >= lastPageRank);
            lastPageRank = pageRank;
            assertFalse(name.equals("lop") || name.equals("ripple"));
            counter++;
        }
        assertEquals(4, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_pageRank_byXpageRankX_asXaX_outXknowsX_pageRank_asXbX_selectXa_bX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_pageRank_byXpageRankX_asXaX_outXknowsX_pageRank_asXbX_selectXa_bX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            Vertex vertex = (Vertex) map.get("a");
            double pageRank = (Double) map.get("b");
            assertEquals(convertToVertexId("marko"), vertex.id());
            assertTrue(pageRank > 0.15d);
            counter++;
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXsoftwareX_hasXname_rippleX_pageRankX1X_byXinEXcreatedXX_timesX1X_byXpriorsX_inXcreatedX_unionXboth__identityX_valueMapXname_priorsX() {
        final Traversal<Vertex, Map<String, List<Object>>> traversal = get_g_V_hasLabelXsoftwareX_hasXname_rippleX_pageRankX1X_byXinEXcreatedXX_timesX1X_byXpriorsX_inXcreatedX_unionXboth__identityX_valueMapXname_priorsX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Map<String, List<Object>> map = traversal.next();
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

    public static class Traversals extends PageRankTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_pageRank() {
            return g.V().pageRank();
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_pageRank_byXoutEXknowsXX_byXfriendRankX_valueMapXname_friendRankX() {
            return g.V().pageRank().by(__.outE("knows")).by("friendRank").valueMap("name", "friendRank");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_pageRank_order_byXpageRank_decrX_name() {
            return g.V().pageRank().order().by(PageRankVertexProgram.PAGE_RANK, Order.decr).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_pageRank_order_byXpageRank_decrX_name_limitX2X() {
            return g.V().pageRank().order().by(PageRankVertexProgram.PAGE_RANK, Order.decr).<String>values("name").limit(2);
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasLabelXpersonX_pageRank_byXpageRankX_order_byXpageRankX_valueMapXname_pageRankX() {
            return g.V().hasLabel("person").pageRank().by("pageRank").order().by("pageRank").valueMap("name", "pageRank");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_pageRank_byXpageRankX_asXaX_outXknowsX_pageRank_asXbX_selectXa_bX() {
            return g.V().pageRank().by("pageRank").as("a").out("knows").values("pageRank").as("b").select("a", "b");
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasLabelXsoftwareX_hasXname_rippleX_pageRankX1X_byXinEXcreatedXX_timesX1X_byXpriorsX_inXcreatedX_unionXboth__identityX_valueMapXname_priorsX() {
            return g.V().hasLabel("software").has("name", "ripple").pageRank(1.0).by(__.inE("created")).times(1).by("priors").in("created").union(__.both(), __.identity()).valueMap("name", "priors");
        }
    }
}
