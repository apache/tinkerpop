package com.tinkerpop.gremlin.process.graph.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyIntervalTestG extends IntervalTest {

    public Traversal<Vertex, Vertex> get_g_v1_outE_intervalXweight_0_06X_inV(final Object v1Id) {
        g.v(v1Id).outE.interval('weight',0.0f,0.6f).inV
    }
}
