package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Element
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyHasTestImpl extends HasTest {

    @Override
    public Traversal<Vertex, Vertex> get_g_v1_hasXkeyX(final Object v1Id, final String key) {
        g.v(v1Id).has(key)
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_v1_hasXname_markoX(final Object v1Id) {
        g.v(v1Id).has('name', 'marko')
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX() {
        g.V.has('name', 'marko')
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_V_hasXname_blahX() {
        g.V.has('name', 'blah')
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_V_hasXblahX() {
        g.V.has('blah')
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_v1_hasXage_gt_30X(final Object v1Id) {
        g.v(v1Id).has('age', T.gt, 30)
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_v1_out_hasXid_2X(final Object v1Id, final Object v2Id) {
        g.v(v1Id).out.has('id', v2Id)
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X() {
        g.V.has('age', T.gt, 30)
    }

    @Override
    public Traversal<Edge, Edge> get_g_e7_hasXlabelXknowsX(final Object e7Id) {
        g.e(e7Id).has(Element.LABEL, 'knows')
    }

    @Override
    public Traversal<Edge, Edge> get_g_E_hasXlabelXknowsX() {
        g.E.has(Element.LABEL, 'knows')
    }

    @Override
    public Traversal<Edge, Edge> get_g_E_hasXlabelXknows_createdX() {
        g.E.has(Element.LABEL, T.in, ['knows', 'created'])
    }

    @Override
    Traversal<Vertex, Vertex> get_g_V_hasXlabelXperson_animalX() {
        return g.V.has(Element.LABEL, T.in, ["person", "animal"]);
    }

    @Override
    public Traversal<Vertex, Vertex> get_g_V_hasXname_equalspredicate_markoX() {
        return g.V().has("name", { v1, v2 -> v1.equals(v2) }, "marko");
    }
}
