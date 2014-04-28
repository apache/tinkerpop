package com.tinkerpop.gremlin.process.graph.filter

import com.tinkerpop.gremlin.groovy.GremlinLoader
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyFilterTest extends FilterTest {

    static {
        GremlinLoader.load();
    }

    public Traversal<Vertex, Vertex> get_g_V_filterXfalseX() {
        g.V.filter { false }
    }

    public Traversal<Vertex, Vertex> get_g_V_filterXtrueX() {
         g.V.filter { true }
    }

    public Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX() {
         g.V.filter { it.get()['lang',null] == 'java' }
    }

    public Traversal<Vertex, Vertex> get_g_v1_out_filterXage_gt_30X(final Object v1Id) {
         g.v(v1Id).out.filter { it.get()['age',0] > 30 }
    }

    public Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
         g.V.filter { it.get()['name'].startsWith('m') || it.get()['name'].startsWith('p') }
    }

}
