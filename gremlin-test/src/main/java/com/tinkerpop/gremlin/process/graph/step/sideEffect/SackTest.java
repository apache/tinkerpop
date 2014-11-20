package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Operator;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class SackTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Float> get_g_V_withSackX0_sumX_asXaX_outE_sackXsum_weightX_inV_jumpXa_2X_sack();

    public abstract Traversal<Vertex, Map> get_g_V_withSackXmap__map_cloneX_out_out_sackXmap_a_nameX_sack();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_withSackX0_sumX_asXaX_outE_sackXsum_weightX_inV_jumpXa_2X_sack() {
        final Traversal<Vertex, Float> traversal = get_g_V_withSackX0_sumX_asXaX_outE_sackXsum_weightX_inV_jumpXa_2X_sack();
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
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Float> get_g_V_withSackX0_sumX_asXaX_outE_sackXsum_weightX_inV_jumpXa_2X_sack() {
            return g.V().withSack(() -> 0.0f, Operator.sum).as("a").outE().sack(Operator.sum, "weight").inV().jump("a", 2).sack();
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
        public Traversal<Vertex, Float> get_g_V_withSackX0_sumX_asXaX_outE_sackXsum_weightX_inV_jumpXa_2X_sack() {
            return g.V().withSack(() -> 0.0f, Operator.sum).as("a").outE().sack(Operator.sum, "weight").inV().jump("a", 2).<Float>sack().submit(g.compute());
        }

        public Traversal<Vertex, Map> get_g_V_withSackXmap__map_cloneX_out_out_sackXmap_a_nameX_sack() {
            return g.V().<HashMap>withSack(HashMap::new, m -> (HashMap) m.clone()).out().out().<Map>sack((map, vertex) -> {
                map.put("a", vertex.value("name"));
                return map;
            }).<Map>sack().submit(g.compute());
        }
    }
}