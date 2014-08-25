package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyWhereTestImpl extends WhereTest {

    @Override
    public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX() {
        return g.V().has('age').as('a').out().in().has('age').as('b').select().where('a', T.eq, 'b');
    }

    @Override
    public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_b_neqX() {
        return g.V().has('age').as('a').out().in().has('age').as('b').select().where('a', 'b') { a, b -> !a.equals(b) };
    }

    @Override
    public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX() {
        return g.V().has('age').as('a').out().in().has('age').as('b').select().where(g.of().as('b').has('name', 'marko'));
    }

    @Override
    public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX() {
        return g.V().has('age').as('a').out().in().has('age').as('b').select().where(g.of().as('a').out('knows').as('b'));
    }
}
