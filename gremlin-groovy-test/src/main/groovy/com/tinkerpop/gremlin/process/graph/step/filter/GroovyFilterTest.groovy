package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyFilterTest {

    public static class StandardTest extends FilterTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXfalseX() {
            g.V.filter { false }
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXtrueX() {
            g.V.filter { true }
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX() {
            g.V.filter { it.get().value('lang', null) == 'java' }
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_filterXage_gt_30X(final Object v1Id) {
            g.v(v1Id).filter { it.age > 30 }
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_filterXage_gt_30X(final Object v1Id) {
            g.v(v1Id).out.filter { it.get().value('age', 0) > 30 }
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
            g.V.filter { it.name.startsWith('m') || it.name.startsWith('p') }
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXfalseX() {
            g.E.filter { false }
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXtrueX() {
            g.E.filter { true }
        }
    }

    public static class ComputerTest extends FilterTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXfalseX() {
            ComputerTestHelper.compute("g.V.filter { false }",g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXtrueX() {
            ComputerTestHelper.compute("g.V.filter { true }",g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX() {
            ComputerTestHelper.compute("g.V.filter { it.get().value('lang', null) == 'java' }",g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_filterXage_gt_30X(final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).filter { it.age > 30 }",g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_filterXage_gt_30X(final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).out.filter { it.get().value('age', 0) > 30 }",g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
            ComputerTestHelper.compute("g.V.filter { it.name.startsWith('m') || it.name.startsWith('p') }",g);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXfalseX() {
            ComputerTestHelper.compute("g.E.filter { false }",g);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXtrueX() {
            ComputerTestHelper.compute("g.E.filter { true }",g);
        }
    }
}
