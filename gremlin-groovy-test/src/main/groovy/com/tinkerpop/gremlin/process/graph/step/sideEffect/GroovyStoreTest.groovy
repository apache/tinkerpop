package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyStoreTest {

    public static class StandardTest extends StoreTest {

        @Override
        public Traversal<Vertex, Collection> get_g_V_storeXa_nameX_out_capXaX() {
            g.V().store('a') { it.name }.out().cap('a');
        }

        @Override
        public Traversal<Vertex, Collection> get_g_v1_storeXa_nameX_out_storeXa_nameX_name_capXaX(final Object v1Id) {
            g.v(v1Id).store('a') { it.name }.out().store('a') { it.name }.name.cap('a');
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXaX_out_storeXaX() {
            g.V.as('a').out.store('a');
        }

        @Override
        public Traversal<Vertex, Set<String>> get_g_V_withXa_setX_both_name_storeXaX() {
            g.V.with('a') { [] as Set }.both.name.store('a')
        }
    }

    public static class ComputerTest extends StoreTest {

        @Override
        public Traversal<Vertex, Collection> get_g_V_storeXa_nameX_out_capXaX() {
            ComputerTestHelper.compute("g.V().store('a') { it.name }.out().cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Collection> get_g_v1_storeXa_nameX_out_storeXa_nameX_name_capXaX(final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).store('a') { it.name }.out().store('a') { it.name }.name.cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXaX_out_storeXaX() {
            ComputerTestHelper.compute("g.V.as('a').out.store('a')", g)
        }

        @Override
        public Traversal<Vertex, Set<String>> get_g_V_withXa_setX_both_name_storeXaX() {
            ComputerTestHelper.compute("g.V.with('a'){[] as Set}.both.name.store('a')", g);
        }
    }
}
