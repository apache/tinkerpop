package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
class GroovyHasNotTestImpl extends HasNotTest {

    @Override
    public Traversal<Vertex, Vertex> get_g_v1_hasNotXprop(final Object v1Id, final String key) {
        g.v(v1Id).hasNot(key)
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String key) {
        g.V.hasNot(key)
    }
}
