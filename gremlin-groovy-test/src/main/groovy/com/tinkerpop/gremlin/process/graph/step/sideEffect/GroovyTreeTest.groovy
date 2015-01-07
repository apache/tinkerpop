package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.process.graph.util.Tree
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyTreeTest {

    public static class StandardTest extends TreeTest {

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_tree_byXidX() {
            g.V.out.out.tree.by(T.id);
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_byXidX() {
            g.V.out.out.tree('a').by(T.id);
        }

        @Override
        public Traversal<Vertex, Tree> get_g_VX1X_out_out_tree_byXnameX(final Object v1Id) {
            g.V(v1Id).out.out.tree.by('name');
        }

        @Override
        public Traversal<Vertex, Tree> get_g_VX1X_out_out_treeXaX_byXnameX_both_both_capXaX(final Object v1Id) {
            g.V(v1Id).out.out.tree('a').by('name').both.both.cap('a');
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX() {
            g.V.out.out.tree("a");
        }
    }

    public static class ComputerTest extends TreeTest {

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_tree_byXidX() {
            ComputerTestHelper.compute("g.V.out.out.tree.by(id)", g)
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_byXidX() {
            ComputerTestHelper.compute("g.V.out.out.tree('a').by(id)", g)
        }

        @Override
        public Traversal<Vertex, Tree> get_g_VX1X_out_out_tree_byXnameX(final Object v1Id) {
            g.V(v1Id).out.out.tree.by('name');
            // TODO
        }

        @Override
        public Traversal<Vertex, Tree> get_g_VX1X_out_out_treeXaX_byXnameX_both_both_capXaX(final Object v1Id) {
            g.V(v1Id).out.out.tree('a').by('name').both.both.cap('a');
            // TODO
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX() {
            ComputerTestHelper.compute("g.V.out.out.tree('a')", g)
        }
    }
}
