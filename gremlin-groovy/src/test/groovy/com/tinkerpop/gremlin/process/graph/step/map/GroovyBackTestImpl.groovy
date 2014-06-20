package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyBackTestImpl extends BackTest {

    public Traversal<Vertex, Vertex> get_g_v1_asXhereX_out_backXhereX(final Object v1Id) {
        g.v(v1Id).as('here').out.back('here')
    }

    public Traversal<Vertex, Vertex> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX(final Object v4Id) {
        g.v(v4Id).out.as('here').has('lang', 'java').back('here')
    }

    public Traversal<Vertex, String> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX(final Object v4Id) {
        g.v(v4Id).out.as('here').has('lang', 'java').back('here').value('name')
    }

    public Traversal<Vertex, Edge> get_g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX(final Object v1Id) {
        g.v(v1Id).outE.as('here').inV.has('name', 'vadas').back('here')
    }

    public Traversal<Vertex, Edge> get_g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX(final Object v1Id) {
        g.v(v1Id).outE('knows').has('weight', 1.0f).as('here').inV.has('name', 'josh').back('here')
    }
}
