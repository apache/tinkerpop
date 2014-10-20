package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyGroupCountTest {

    public static class StandardTest extends GroupCountTest {

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_groupCountXnameX() {
            g.V().out('created').groupCount { it.name }
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_name_groupCount() {
            g.V().out('created').name.groupCount
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_name_groupCountXaX() {
            g.V().out('created').name.groupCount('a')
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_filterXfalseX_groupCount() {
            g.V().filter { false }.groupCount;
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_capXaX() {
            g.V().as('x').out().groupCount('a') { it.name }.jump('x') { it.loops() < 2 }.cap('a')
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_2X_capXaX() {
            g.V().as('x').out().groupCount('a') { it.name }.jump('x', 2).cap('a')
        }
    }

    public static class ComputerTest extends GroupCountTest {

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_groupCountXnameX() {
            ComputerTestHelper.compute("g.V().out('created').groupCount { it.name }", g)
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_name_groupCount() {
            ComputerTestHelper.compute("g.V().out('created').name.groupCount", g)
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_name_groupCountXaX() {
            ComputerTestHelper.compute("g.V().out('created').name.groupCount('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_filterXfalseX_groupCount() {
            ComputerTestHelper.compute("g.V().filter { false }.groupCount", g)
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_capXaX() {
            ComputerTestHelper.compute("g.V().as('x').out().groupCount('a') { it.name }.jump('x') { it.loops() < 2 }.cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_2X_capXaX() {
            ComputerTestHelper.compute("g.V().as('x').out().groupCount('a') { it.name }.jump('x', 2).cap('a')", g)
        }
    }
}
