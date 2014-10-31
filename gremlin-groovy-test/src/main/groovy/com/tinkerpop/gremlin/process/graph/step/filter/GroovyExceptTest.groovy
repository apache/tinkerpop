package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex
import org.junit.Ignore

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 */
public abstract class GroovyExceptTest {

    public static class StandardTest extends ExceptTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_exceptXg_v2X(final Object v1Id, final Object v2Id) {
            g.v(v1Id).out.except(g.v(v2Id))
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_exceptXxX(final Object v1Id) {
            g.v(v1Id).out.aggregate('x').out.except('x')
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_name(final Object v1Id) {
            g.v(v1Id).out('created').in('created').except(g.v(v1Id)).values('name')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_exceptXg_V_toListX() {
            g.V.out.except(g.V.toList())
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_exceptXX() {
            g.V.out.except([])
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_asXxX_bothEXcreatedX_exceptXeX_aggregateXeX_otherV_jumpXx_true_trueX_path(
                final Object v1Id) {
            g.v(v1Id).as('x').bothE("created").except('e').aggregate('e').otherV.jump('x') { true } { true }.path
        }
    }

    public static class ComputerTest extends ExceptTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_exceptXg_v2X(final Object v1Id, final Object v2Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).out.except(g.v(${v2Id}))", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_exceptXxX(final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).out.aggregate('x').out.except('x')", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_name(final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).out('created').in('created').except(g.v(${v1Id})).values('name')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_exceptXg_V_toListX() {
            ComputerTestHelper.compute("g.V.out.except(g.V.toList())", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_exceptXX() {
            ComputerTestHelper.compute("g.V.out.except([])", g);
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_asXxX_bothEXcreatedX_exceptXeX_aggregateXeX_otherV_jumpXx_true_trueX_path(
                final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).as('x').bothE('created').except('e').aggregate('e').otherV.jump('x') { true } { true }.path", g);
        }
    }
}
