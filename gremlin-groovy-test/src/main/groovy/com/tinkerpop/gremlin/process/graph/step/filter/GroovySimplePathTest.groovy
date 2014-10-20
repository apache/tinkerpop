package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySimplePathTest {

    public static class StandardTest extends SimplePathTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_simplePath(final Object v1Id) {
            g.v(v1Id).out('created').in('created').simplePath
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_both_simplePath_jumpXx_loops_lt_3X_path() {
            return g.V.as("x").both.simplePath().jump('x') { it.loops() < 3 }.path
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_both_simplePath_jumpXx_3X_path() {
            return g.V.as("x").both().simplePath.jump('x', 3).path()
        }
    }

    public static class ComputerTest extends SimplePathTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_simplePath(final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).out('created').in('created').simplePath", g);
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_both_simplePath_jumpXx_loops_lt_3X_path() {
            ComputerTestHelper.compute("g.V.as('x').both.simplePath().jump('x') { it.loops() < 3 }.path", g);
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_both_simplePath_jumpXx_3X_path() {
            ComputerTestHelper.compute("g.V.as('x').both().simplePath.jump('x', 3).path()", g);
        }
    }
}
