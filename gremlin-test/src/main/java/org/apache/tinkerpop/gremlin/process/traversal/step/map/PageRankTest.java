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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class PageRankTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_pageRank();

    public abstract Traversal<Vertex, String> get_g_V_pageRank_order_byXpageRank_decrX_name();

    public abstract Traversal<Vertex, String> get_g_V_pageRank_order_byXpageRank_decrX_name_limitX2X();

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
    public void g_V_pageRank_order_byXpageRank_decrX_name_limitX2X() {
        final Traversal<Vertex, String> traversal = get_g_V_pageRank_order_byXpageRank_decrX_name_limitX2X();
        printTraversalForm(traversal);
        final List<String> names = traversal.toList();
        assertEquals(2, names.size());
        assertEquals("lop", names.get(0));
        assertEquals("ripple", names.get(1));
    }

    public static class Traversals extends PageRankTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_pageRank() {
            return g.V().pageRank();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_pageRank_order_byXpageRank_decrX_name() {
            return g.V().pageRank().order().by(PageRankVertexProgram.PAGE_RANK, Order.decr).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_pageRank_order_byXpageRank_decrX_name_limitX2X() {
            return g.V().pageRank().order().by(PageRankVertexProgram.PAGE_RANK, Order.decr).<String>values("name").limit(2);
        }
    }
}
