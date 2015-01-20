package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyRangeTest {

    public static class StandardTest extends RangeTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_limitX2X(final Object v1Id) {
            g.V(v1Id).out.limit(2)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_localXoutE_limitX1X_inVX_limitX3X() {
            g.V.local(__.outE.limit(3)).inV.limit(3)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(final Object v1Id) {
            g.V(v1Id).out('knows').outE('created')[0].inV()
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(final Object v1Id) {
            g.V(v1Id).out('knows').out('created')[0]
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(final Object v1Id) {
            g.V(v1Id).out('created').in('created')[1..3]
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(final Object v1Id) {
            g.V(v1Id).out('created').inE('created')[1..3].outV
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_rangeX5_11X() {
            g.V().repeat(__.both).times(3)[5..11];
        }
    }

    public static class ComputerTestImpl extends RangeTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_limitX2X(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out.limit(2)", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_localXoutE_limitX1X_inVX_limitX3X() {
            ComputerTestHelper.compute("g.V.local(__.outE.limit(3)).inV.limit(3)", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out('knows').outE('created')[0].inV()", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out('knows').out('created')[0]", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out('created').in('created')[1..3]", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out('created').inE('created')[1..3].outV", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_rangeX5_11X() {
            ComputerTestHelper.compute("g.V().repeat(__.both).times(3)[5..11]", g);
        }
    }
}
