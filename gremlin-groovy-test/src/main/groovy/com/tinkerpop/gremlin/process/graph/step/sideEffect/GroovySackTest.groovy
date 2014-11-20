package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.structure.Operator.sum;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySackTest {

    public static class StandardTest extends SackTest {
        @Override
        public Traversal<Vertex, Float> get_g_V_withSackX0_sumX_asXaX_outE_sackXsum_weightX_inV_jumpXa_2X_sack() {
            g.V.withSack(0.0f, sum).as('a').outE.sack(sum, 'weight').inV.jump('a', 2).sack
        }
    }

    public static class ComputerTest extends SackTest {
        @Override
        public Traversal<Vertex, Float> get_g_V_withSackX0_sumX_asXaX_outE_sackXsum_weightX_inV_jumpXa_2X_sack() {
            ComputerTestHelper.compute(" g.V.withSack(0.0f, sum).as('a').outE.sack(sum, 'weight').inV.jump('a', 2).sack", g)
        }
    }
}
