package com.tinkerpop.gremlin.process.graph.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
class GroovyHasNotTestImpl extends HasNotTest {

    public Traversal<Vertex, Vertex> get_g_v1_hasNotXprop(final Object v1Id, final String prop) {
        g.v(v1Id).hasNot(prop)
    }

    public Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String prop) {
        g.V.hasNot(prop)
    }
}
