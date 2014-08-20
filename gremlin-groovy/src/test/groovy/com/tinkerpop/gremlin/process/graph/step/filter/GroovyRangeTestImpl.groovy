package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyRangeTestImpl extends RangeTest {

    @Override
    public Traversal<Vertex, Vertex> get_g_v1_out_rangeX0_1X(final Object v1Id) {
        g.v(v1Id).out[0..1]
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_V_outX1X_rangeX0_2X() {
        g.V.out(1)[0..2]
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV(final Object v1Id) {
        g.v(v1Id).out('knows').outE('created')[0..0].inV
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_v1_outXknowsX_outXcreatedX_rangeX0_0X(final Object v1Id) {
        g.v(v1Id).out('knows').out('created')[0..0]
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_rangeX1_2X(final Object v1Id) {
        g.v(v1Id).out('created').in('created')[1..2]
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV(final Object v1Id) {
        g.v(v1Id).out('created').inE('created')[1..2].outV
    }
}
