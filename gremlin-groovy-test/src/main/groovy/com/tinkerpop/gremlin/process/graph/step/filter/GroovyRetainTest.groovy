package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyRetainTest {

    public static class StandardTest extends RetainTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_retainXg_v2X(final Object v1Id, final Object v2Id) {
            g.V(v1Id).out.retain(g.V(v2Id).next())
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_retainXxX(final Object v1Id) {
            g.V(v1Id).out.aggregate('x').out.retain('x')
        }
    }

    public static class ComputerTest extends RetainTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_retainXg_v2X(final Object v1Id, final Object v2Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out.retain(g.V($v2Id).next())", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_retainXxX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out.aggregate('x').out.retain('x')", g);
        }
    }
}
