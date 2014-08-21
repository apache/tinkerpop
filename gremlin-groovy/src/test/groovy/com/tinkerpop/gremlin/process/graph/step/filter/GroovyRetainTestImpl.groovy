package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyRetainTestImpl extends RetainTest {

    @Override
    public Traversal<Vertex, Vertex> get_g_v1_out_retainXg_v2X(final Object v1Id, final Object v2Id) {
        g.v(v1Id).out().retain(g.v(v2Id))
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_retainXxX(final Object v1Id) {
        g.v(v1Id).out().aggregate('x').out().retain('x')
    }
}
