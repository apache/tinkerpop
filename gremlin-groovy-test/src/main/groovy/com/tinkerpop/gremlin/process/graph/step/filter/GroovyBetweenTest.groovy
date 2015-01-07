package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyBetweenTest {

    public static class StandardTest extends BetweenTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_betweenXweight_0_06X_inV(final Object v1Id) {
            g.V(v1Id).outE.between('weight', 0.0d, 0.6d).inV
        }
    }

    public static class ComputerTest extends BetweenTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_betweenXweight_0_06X_inV(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE.between('weight', 0.0d, 0.6d).inV", g);
        }
    }
}
