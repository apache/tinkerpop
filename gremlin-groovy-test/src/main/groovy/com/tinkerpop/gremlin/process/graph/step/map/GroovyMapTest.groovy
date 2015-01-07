package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GroovyMapTest {

    public static class StandardTest extends MapTest {

        @Override
        public Traversal<Vertex, String> get_g_VX1X_mapXnameX(final Object v1Id) {
            g.V(v1Id).map { v -> v.name };
        }

        @Override
        public Traversal<Vertex, Integer> get_g_VX1X_outE_label_mapXlengthX(final Object v1Id) {
            g.V(v1Id).outE.label.map { l -> l.length() };
        }

        @Override
        public Traversal<Vertex, Integer> get_g_VX1X_out_mapXnameX_mapXlengthX(final Object v1Id) {
            g.V(v1Id).out.map { v -> v.name }.map { n -> n.length() };
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_mapXa_nameX() {
            g.V.as('a').out.map { v -> v.path('a').name };
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_out_mapXa_name_it_nameX() {
            g.V().as('a').out.out().map { v -> v.path('a').name + v.name };
        }
    }

    public static class ComputerTest extends MapTest {

        @Override
        public Traversal<Vertex, String> get_g_VX1X_mapXnameX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).map { v -> v.name }", g);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_VX1X_outE_label_mapXlengthX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE.label.map { l -> l.length() }", g);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_VX1X_out_mapXnameX_mapXlengthX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out.map { v -> v.name }.map { n -> n.length() }", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_mapXa_nameX() {
            // TODO: Doesn't work for graph computer because sideEffects are not accessible
            g.V.as('a').out.map { v -> v.path('a').name };
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_out_mapXa_name_it_nameX() {
            // TODO: Doesn't work for graph computer because sideEffects are not accessible
            g.V().as('a').out.out().map { v -> v.path('a').name + v.name };
        }
    }
}
