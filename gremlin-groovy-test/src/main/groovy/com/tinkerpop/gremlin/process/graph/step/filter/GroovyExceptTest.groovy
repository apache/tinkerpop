package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 */
public abstract class GroovyExceptTest {

    public static class StandardTest extends ExceptTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_exceptXg_v2X(final Object v1Id, final Object v2Id) {
            g.V(v1Id).out.except(g.V(v2Id).next())
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_aggregateXxX_out_exceptXxX(final Object v1Id) {
            g.V(v1Id).out.aggregate('x').out.except('x')
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_outXcreatedX_inXcreatedX_exceptXg_v1X_name(final Object v1Id) {
            g.V(v1Id).out('created').in('created').except(g.V(v1Id).next()).values('name')
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
        public Traversal<Vertex, Path> get_g_VX1X_repeatXbothEXcreatedX_exceptXeX_aggregateXeX_otherVX_emit_path(
                final Object v1Id) {
            g.V(v1Id).as('x').bothE("created").except('e').aggregate('e').otherV.jump('x') { true } { true }.path
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_exceptXaX_name(final Object v1Id) {
            g.V(v1Id).as('a').out('created').in('created').except('a').name
        }
    }

    public static class ComputerTest extends ExceptTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_exceptXg_v2X(final Object v1Id, final Object v2Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out.except(g.V(${v2Id}).next())", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_aggregateXxX_out_exceptXxX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out.aggregate('x').out.except('x')", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_outXcreatedX_inXcreatedX_exceptXg_v1X_name(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out('created').in('created').except(g.V(${v1Id}).next()).values('name')", g);
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
        public Traversal<Vertex, Path> get_g_VX1X_repeatXbothEXcreatedX_exceptXeX_aggregateXeX_otherVX_emit_path(
                final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).as('x').bothE('created').except('e').aggregate('e').otherV.jump('x') { true } { true }.path", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_exceptXaX_name(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).as('a').out('created').in('created').except('a').name", g);
        }
    }
}
