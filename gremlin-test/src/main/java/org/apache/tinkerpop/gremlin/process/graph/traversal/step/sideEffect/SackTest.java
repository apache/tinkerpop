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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.structure.Operator;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.graph.traversal.__.*;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class SackTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Double> get_g_V_withSackX0X_outE_sackXsum_weightX_inV_sack_sum();

    public abstract Traversal<Vertex, Float> get_g_V_withSackX0X_repeatXoutE_sackXsum_weightX_inVX_timesX2X_sack();

    public abstract Traversal<Vertex, Map> get_g_V_withSackXmap__map_cloneX_out_out_sackXmap_a_nameX_sack();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_withSackX0X_outE_sackXsum_weightX_inV_sack_sum() {
        final Traversal<Vertex, Double> traversal = get_g_V_withSackX0X_outE_sackXsum_weightX_inV_sack_sum();
        assertEquals(3.5d, traversal.next(), 0.00001d);
        assertFalse(traversal.hasNext());
    }


    @Test
    @LoadGraphWith(MODERN)
    public void g_V_withSackX0X_repeatXoutE_sackXsum_weightX_inVX_timesX2X_sack() {
        final Traversal<Vertex, Float> traversal = get_g_V_withSackX0X_repeatXoutE_sackXsum_weightX_inVX_timesX2X_sack();
        super.checkResults(Arrays.asList(2.0f, 1.4f), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_withSackXmap__map_cloneX_out_out_sackXmap_a_nameX_sack() {
        final Traversal<Vertex, Map> traversal = get_g_V_withSackXmap__map_cloneX_out_out_sackXmap_a_nameX_sack();
        int counter = 0;
        while (traversal.hasNext()) {
            final Map map = traversal.next();
            assertEquals(1, map.size());
            assertTrue(map.containsKey("a"));
            assertTrue(map.get("a").equals("ripple") || map.get("a").equals("lop"));
            counter++;
        }
        assertEquals(2, counter);
    }

    public static class StandardTest extends SackTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_withSackX0X_outE_sackXsum_weightX_inV_sack_sum() {
            return g.V().withSack(0.0f).outE().sack(Operator.sum, "weight").inV().sack().sum();
        }

        @Override
        public Traversal<Vertex, Float> get_g_V_withSackX0X_repeatXoutE_sackXsum_weightX_inVX_timesX2X_sack() {
            return g.V().withSack(0.0f).repeat(outE().sack(Operator.sum, "weight").inV()).times(2).sack();
        }

        @Override
        public Traversal<Vertex, Map> get_g_V_withSackXmap__map_cloneX_out_out_sackXmap_a_nameX_sack() {
            return g.V().<HashMap>withSack(HashMap::new, m -> (HashMap) m.clone()).out().out().<Map>sack((map, vertex) -> {
                map.put("a", vertex.value("name"));
                return map;
            }).sack();
        }
    }

    public static class ComputerTest extends SackTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_withSackX0X_outE_sackXsum_weightX_inV_sack_sum() {
            return g.V().withSack(0.0f).outE().sack(Operator.sum, "weight").inV().sack().sum().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Float> get_g_V_withSackX0X_repeatXoutE_sackXsum_weightX_inVX_timesX2X_sack() {
            return g.V().withSack(0.0f).repeat(outE().sack(Operator.sum, "weight").inV()).times(2).<Float>sack().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map> get_g_V_withSackXmap__map_cloneX_out_out_sackXmap_a_nameX_sack() {
            return g.V().<HashMap>withSack(HashMap::new, m -> (HashMap) m.clone()).out().out().<Map>sack((map, vertex) -> {
                map.put("a", vertex.value("name"));
                return map;
            }).<Map>sack(); // TODO: Make it so the .submit(g.compute());
        }
    }
}