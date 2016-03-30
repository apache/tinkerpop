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
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ProgramTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_programXpageRankX();

    public abstract Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasLabelXpersonX_programXpageRank_rankX_order_byXrank_incrX_valueMapXname_rankX();


    @Test
    @LoadGraphWith(MODERN)
    public void g_V_programXpageRankX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_programXpageRankX();
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
    public void g_V_hasLabelXpersonX_programXpageRank_rankX_order_byXrank_decrX_valueMapXname_rankX() {
        final Traversal<Vertex, Map<String, List<Object>>> traversal = get_g_V_hasLabelXpersonX_programXpageRank_rankX_order_byXrank_incrX_valueMapXname_rankX();
        printTraversalForm(traversal);
        int counter = 0;
        double lastRank = Double.MIN_VALUE;
        while (traversal.hasNext()) {
            final Map<String, List<Object>> map = traversal.next();
            assertEquals(2, map.size());
            assertEquals(1, map.get("name").size());
            assertEquals(1, map.get("rank").size());
            String name = (String) map.get("name").get(0);
            double rank = (Double) map.get("rank").get(0);
            assertTrue(rank >= lastRank);
            lastRank = rank;
            assertFalse(name.equals("lop") || name.equals("ripple"));
            counter++;
        }
        assertEquals(4, counter);
    }


    public static class Traversals extends ProgramTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_programXpageRankX() {
            return g.V().program(PageRankVertexProgram.build().create(graph));
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasLabelXpersonX_programXpageRank_rankX_order_byXrank_incrX_valueMapXname_rankX() {
            return g.V().hasLabel("person").program(PageRankVertexProgram.build().property("rank").create(graph)).order().by("rank", Order.incr).valueMap("name", "rank");
        }
    }
}