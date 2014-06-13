package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.filter.FilterTest
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyFilterTestImpl extends FilterTest {

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
         g.V.filter { it.get()['lang',null] == 'java' }
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_v1_filterXage_gt_30X(Object v1Id) {
        g.v(v1Id).filter { it.get()['age',0] > 30 }
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_v1_out_filterXage_gt_30X(final Object v1Id) {
         g.v(v1Id).out.filter { it.get()['age',0] > 30 }
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
         g.V.filter { it.get()['name'].startsWith('m') || it.get()['name'].startsWith('p') }
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
