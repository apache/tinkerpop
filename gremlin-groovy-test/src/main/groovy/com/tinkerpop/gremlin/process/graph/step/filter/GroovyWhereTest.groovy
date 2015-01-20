package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Compare
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyWhereTest {

    public static class StandardTest extends WhereTest {
        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX() {
            g.V.has('age').as('a').out.in.has('age').as('b').select().where('a', Compare.eq, 'b');
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_b_neqX() {
            g.V.has('age').as('a').out.in.has('age').as('b').select().where('a', 'b') { a, b -> !a.equals(b) };
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX() {
            g.V.has('age').as('a').out.in.has('age').as('b').select().where(__.as('b').has('name', 'marko'));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX() {
            g.V().has('age').as('a').out.in.has('age').as('b').select().where(__.as('a').out('knows').as('b'));
        }
    }

    public static class ComputerTest extends WhereTest {
        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX() {
            ComputerTestHelper.compute("g.V.has('age').as('a').out.in.has('age').as('b').select().where('a', Compare.eq, 'b')", g);
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_b_neqX() {
            ComputerTestHelper.compute("g.V.has('age').as('a').out.in.has('age').as('b').select().where('a', 'b') { a, b -> !a.equals(b) }", g);
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX() {
            // TODO: ComputerTestHelper.compute("g.V.has('age').as('a').out.in.has('age').as('b').select().where(__.as('b').has('name', 'marko'))", g);
            g.V.has('age').as('a').out.in.has('age').as('b').select().where(__.as('b').has('name', 'marko'));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX() {
            // TODO: ComputerTestHelper.compute("g.V().has('age').as('a').out.in.has('age').as('b').select().where(__.as('a').out('knows').as('b'))", g);
            g.V().has('age').as('a').out.in.has('age').as('b').select().where(__.as('a').out('knows').as('b'));
        }
    }
}
