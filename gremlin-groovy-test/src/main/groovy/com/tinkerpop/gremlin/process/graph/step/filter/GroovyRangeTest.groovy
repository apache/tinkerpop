package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyRangeTest {

    public static class StandardTest extends RangeTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_limitX2X(final Object v1Id) {
            g.v(v1Id).out.limit(2)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_outE_localLimitX1X_inV_limitX3X() {
            g.V.outE.localLimit(3).inV.limit(3)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXknowsX_outEXcreatedX_rangeX0_1X_inV(final Object v1Id) {
            g.v(v1Id).out('knows').outE('created')[0].inV()
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXknowsX_outXcreatedX_rangeX0_1X(final Object v1Id) {
            g.v(v1Id).out('knows').out('created')[0]
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_rangeX1_3X(final Object v1Id) {
            g.v(v1Id).out('created').in('created')[1..3]
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(final Object v1Id) {
            g.v(v1Id).out('created').inE('created')[1..3].outV
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXaX_both_jumpXa_3X_rangeX5_11X() {
            g.V().as('a').both().jump('a', 3)[5..11];
        }
    }

    public static class ComputerTestImpl extends RangeTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_limitX2X(final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).out.limit(2)", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_outE_localLimitX1X_inV_limitX3X() {
            ComputerTestHelper.compute("g.V.out(1).limit(3)", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXknowsX_outEXcreatedX_rangeX0_1X_inV(final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).out('knows').outE('created')[0].inV()", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXknowsX_outXcreatedX_rangeX0_1X(final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).out('knows').out('created')[0]", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_rangeX1_3X(final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).out('created').in('created')[1..3]", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).out('created').inE('created')[1..3].outV", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXaX_both_jumpXa_3X_rangeX5_11X() {
            ComputerTestHelper.compute("g.V().as('a').both().jump('a', 3).range(5, 11)", g);
        }
    }
}
