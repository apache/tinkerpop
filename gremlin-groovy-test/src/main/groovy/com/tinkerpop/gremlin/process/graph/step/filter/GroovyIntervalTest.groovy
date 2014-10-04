package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyIntervalTest {

    public static class StandardTest extends IntervalTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outE_intervalXweight_0_06X_inV(final Object v1Id) {
            g.v(v1Id).outE.interval('weight', 0.0d, 0.6d).inV
        }
    }

    public static class ComputerTest extends IntervalTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outE_intervalXweight_0_06X_inV(final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).outE.interval('weight', 0.0d, 0.6d).inV", g);
        }
    }
}
