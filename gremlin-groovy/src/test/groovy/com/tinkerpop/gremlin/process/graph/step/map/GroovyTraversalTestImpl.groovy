package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.map.TraversalTest
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyTraversalTestImpl extends TraversalTest {

    public Traversal<Vertex, Vertex> get_g_V() {
        g.V
    }

    public Traversal<Vertex, Vertex> get_g_v1_out(final Object v1Id) {
        g.v(v1Id).out
    }

    public Traversal<Vertex, Vertex> get_g_v2_in(final Object v2Id) {
        g.v(v2Id).in
    }

    public Traversal<Vertex, Vertex> get_g_v4_both(final Object v4Id) {
        g.v(v4Id).both()
    }

    public Traversal<Vertex, String> get_g_v1_outX1_knowsX_name(final Object v1Id) {
        g.v(v1Id).out(1, 'knows').name
    }

    public Traversal<Vertex, String> get_g_V_bothX1_createdX_name() {
        g.V.both(1, 'created').name
    }

    public Traversal<Edge, Edge> get_g_E() {
        g.E
    }

    public Traversal<Vertex, Edge> get_g_v1_outE(final Object v1Id) {
        g.v(v1Id).outE
    }

    public Traversal<Vertex, Edge> get_g_v2_inE(final Object v2Id) {
        g.v(v2Id).inE
    }

    public Traversal<Vertex, Edge> get_g_v4_bothE(final Object v4Id) {
        g.v(v4Id).bothE
    }

    public Traversal<Vertex, Edge> get_g_v4_bothEX1_createdX(final Object v4Id) {
        g.v(v4Id).bothE(1, 'created')
    }

    public Traversal<Vertex, String> get_g_V_inEX2_knowsX_outV_name() {
        g.V.inE(2, 'knows').outV.name
    }

    public Traversal<Vertex, Vertex> get_g_v1_outE_inV(final Object v1Id) {
        g.v(v1Id).outE.inV
    }

    public Traversal<Vertex, Vertex> get_g_v2_inE_outV(final Object v2Id) {
        g.v(v2Id).inE.outV
    }

    public Traversal<Vertex, Vertex> get_g_V_outE_hasXweight_1X_outV() {
        g.V.outE.has('weight', 1.0f).outV
    }

    public Traversal<Vertex, String> get_g_V_out_outE_inV_inE_inV_both_name() {
        g.V.out.outE.inV.inE.inV.both.name
    }

    public Traversal<Vertex, String> get_g_v1_outEXknowsX_bothV_name(final Object v1Id) {
        g.v(v1Id).outE('knows').bothV.name
    }

    public Traversal<Vertex, Vertex> get_g_v1_outXknowsX(final Object v1Id) {
        g.v(v1Id).out('knows')
    }

    public Traversal<Vertex, Vertex> get_g_v1_outXknows_createdX(final Object v1Id) {
        g.v(v1Id).out('knows', 'created')
    }

    public Traversal<Vertex, Vertex> get_g_v1_outEXknowsX_inV(final Object v1Id) {
        g.v(v1Id).outE('knows').inV()
    }

    public Traversal<Vertex, Vertex> get_g_v1_outEXknows_createdX_inV(final Object v1Id) {
        g.v(v1Id).outE('knows', 'created').inV
    }

    public Traversal<Vertex, Vertex> get_g_V_out_out() {
        g.V.out.out
    }

    public Traversal<Vertex, Vertex> get_g_v1_out_out_out(final Object v1Id) {
        g.v(v1Id).out.out.out
    }

    public Traversal<Vertex, String> get_g_v1_out_valueXnameX(final Object v1Id) {
        g.v(v1Id).out.value('name')
    }

    public Traversal<Vertex, Vertex> get_g_v1_outE_otherV(final Object v1Id) {
        g.v(v1Id).outE.otherV
    }

    public Traversal<Vertex, Vertex> get_g_v4_bothE_otherV(final Object v4Id) {
        g.v(v4Id).bothE.otherV
    }

    public Traversal<Vertex, Vertex> get_g_v4_bothE_hasXweight_lt_1X_otherV(final Object v4Id) {
        g.v(v4Id).bothE.has('weight', T.lt, 1.0f).otherV
    }
}
