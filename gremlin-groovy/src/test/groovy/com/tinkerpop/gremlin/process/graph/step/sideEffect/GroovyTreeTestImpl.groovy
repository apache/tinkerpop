package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.util.Tree
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyTreeTestImpl extends TreeTest {

    @Override
    public Traversal<Vertex, Tree> get_g_V_out_out_treeXidX() {
        g.V().out().out().tree { it.id() };
    }

    @Override
    public Traversal<Vertex, Tree> get_g_V_out_out_treeXa_idX() {
        g.V().out().out().tree('a') { it.id() };
    }

    @Override
    public Traversal<Vertex, Tree> get_g_v1_out_out_treeXnameX(final Object v1Id) {
        g.v(v1Id).out().out().tree { it.value("name") };
    }

    @Override
    public Traversal<Vertex, Tree> get_g_v1_out_out_treeXa_nameX_both_both_capXaX(final Object v1Id) {
        g.v(v1Id).out().out().tree('a') { it.value("name") }.both().both().cap('a');
    }

    @Override
    public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX() {
        g.V().out().out().tree("a");
    }
}
