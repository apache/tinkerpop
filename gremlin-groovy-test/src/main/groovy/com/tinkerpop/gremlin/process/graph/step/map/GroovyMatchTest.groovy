package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Compare
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyMatchTest {

    public static class StandardTest extends MatchTest {

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_out_bX() {
            g.V().match('a', g.of().as('a').out().as('b'));
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_matchXa_out_bX_selectXb_idX() {
            g.V().match('a', g.of().as('a').out().as('b')).select('b') { it.id() };
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__b_created_cX() {
            g.V().match('a',
                    g.of().as('a').out('knows').as('b'),
                    g.of().as('b').out('created').as('c'))
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__a_created_cX() {
            return g.V().match('a',
                    g.of().as('a').out('knows').as('b'),
                    g.of().as('a').out('created').as('c'));
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXd_0knows_a__d_hasXname_vadasX__a_knows_b__b_created_cX() {
            return g.V().match('d',
                    g.of().as('d').in('knows').as('a'),
                    g.of().as('d').has('name', 'vadas'),
                    g.of().as('a').out('knows').as('b'),
                    g.of().as('b').out('created').as('c'));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__a_out_jump2_bX_selectXab_nameX() {
            g.V().match('a',
                    g.of().as('a').out('created').as('b'),
                    g.of().as('a').out().jump('a', 2).as('b')).select(['a', 'b']) { it.name }
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_lop_b__b_0created_29_c__c_out_jump2_cX_selectXnameX() {
            g.V().match('a',
                    g.of().as('a').out('created').has('name', 'lop').as('b'),
                    g.of().as('b').in('created').has('age', 29).as('c'),
                    g.of().as('c').out().jump('c') { it.loops() < 2 }).select { it.name }
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_out_matchXa_0created_b__b_0knows_cX_selectXcX_outXcreatedX_name() {
            g.V().out().out().match('a',
                    g.of().as('a').in('created').as('b'),
                    g.of().as('b').in('knows').as('c')).select('c').out('created').name
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_matchXa_created_b__b_0created_aX() {
            g.V().match('a',
                    g.of().as('a').out('created').as('b'),
                    g.of().as('b').in('created').as('a'))
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_matchXa_knows_b__c_knows_bX() {
            return g.V().match('a', g.of().as('a').out('knows').as('b'),
                    g.of().as('c').out('knows').as('b'));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_knows_b__b_created_lop__b_matchXa1_created_b1__b1_0created_c1X_selectXc1X_cX_selectXnameX() {
            g.V().match('a',
                    g.of().as('a').out('knows').as('b'),
                    g.of().as('b').out('created').has('name', 'lop'),
                    g.of().as('b').match('a1',
                            g.of().as('a1').out('created').as('b1'),
                            g.of().as('b1').in('created').as('c1')).select('c1').as('c')).select { it.name }
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX() {
            g.V().match('a',
                    g.of().as('a').has('name', 'Garcia'),
                    g.of().as('a').in('writtenBy').as('b'),
                    g.of().as('a').in('sungBy').as('b'));
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX() {
            g.V().match('a',
                    g.of().as('a').in('sungBy').as('b'),
                    g.of().as('a').in('sungBy').as('c'),
                    g.of().as('b').out('writtenBy').as('d'),
                    g.of().as('c').out('writtenBy').as('e'),
                    g.of().as('d').has('name', 'George_Harrison'),
                    g.of().as('e').has('name', 'Bob_Marley'));
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX() {
            g.V().match('a',
                    g.of().as('a').in('sungBy').as('b'),
                    g.of().as('a').in('writtenBy').as('c'),
                    g.of().as('b').out('writtenBy').as('d'),
                    g.of().as('c').out('sungBy').as('d'),
                    g.of().as('d').has('name', 'Garcia'));
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_dX_whereXc_sungBy_dX_whereXd_hasXname_GarciaXX() {
            return g.V().match('a',
                    g.of().as('a').in('sungBy').as('b'),
                    g.of().as('a').in('writtenBy').as('c'),
                    g.of().as('b').out('writtenBy').as('d'))
                    .where(g.of().as('c').out('sungBy').as('d'))
                    .where(g.of().as('d').has('name', 'Garcia'));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_out_jump2_cX_selectXnameX() {
            g.V().match("a",
                    g.of().as("a").out("created").has("name", "lop").as("b"),
                    g.of().as("b").in("created").has("age", 29).as("c"))
                    .where(g.of().as("c").out().jump("c") { it.loops() < 2 })
                    .select { it.name };
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__b_0created_cX_whereXa_neq_cX_selectXa_c_nameX() {
            g.V().match('a',
                    g.of().as('a').out('created').as('b'),
                    g.of().as('b').in('created').as('c'))
                    .where('a', Compare.neq, 'c')
                    .select(['a', 'c']) { it.name }
        }

        /*@Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__c_created_bX_selectXnameX() {
            g.V.match('a',
                    g.of().as('a').out('created').as('b'),
                    g.of().as('c').out('created').as('b')).select { it.value('name') }
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_out_hasXname_rippleX_matchXb_created_a__c_knows_bX_selectXcX_outXknowsX_name() {
            g.V.out.out.match('a',
                    g.of().as('b').out('created').as('a'),
                    g.of().as('c').out('knows').as('b')).select('c').out('knows').name
        }*/
    }

}