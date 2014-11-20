package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Operator;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class SackTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Float> get_g_V_withSackX0_sumX_asXaX_outE_sackXsum_weightX_inV_jumpXa_2X_sack();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_withSackX0_sumX_asXaX_outE_sackXsum_weightX_inV_jumpXa_2X_sack() {
        final Traversal<Vertex, Float> traversal = get_g_V_withSackX0_sumX_asXaX_outE_sackXsum_weightX_inV_jumpXa_2X_sack();
        super.checkResults(Arrays.asList(2.0f, 1.4f), traversal);
    }

    public static class StandardTest extends SackTest {
        public StandardTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Float> get_g_V_withSackX0_sumX_asXaX_outE_sackXsum_weightX_inV_jumpXa_2X_sack() {
            return g.V().withSack(0.0f, Operator.sum).as("a").outE().sack(Operator.sum, "weight").inV().jump("a", 2).sack();
        }
    }

    public static class ComputerTest extends SackTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Float> get_g_V_withSackX0_sumX_asXaX_outE_sackXsum_weightX_inV_jumpXa_2X_sack() {
            return g.V().withSack(0.0f, Operator.sum).as("a").outE().sack(Operator.sum, "weight").inV().jump("a", 2).<Float>sack().submit(g.compute());
        }
    }
}