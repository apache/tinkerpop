package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyMatchTestImpl extends MatchTest {
    public GroovyMapTestImpl() { requiresGraphComputer = false; }

    @Override
    public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_out_bX() {
        g.V.match('a', g.of().as('a').out.as('b'));
    }

    @Override
    public Traversal<Vertex, Object> get_g_V_matchXa_out_bX_selectXb_idX() {
        g.V.match('a', g.of().as('a').out.as('b')).select('b') { it.id };
    }

    @Override
    public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__b_created_cX() {
        g.V().match('a',
                g.of().as('a').out('knows').as('b'),
                g.of().as('b').out('created').as('c'))
    }

    @Override
    public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__a_out_jump2_bX_selectXab_nameX() {
        g.V.match('a',
                g.of().as('a').out('created').as('b'),
                g.of().as('a').out().jump('a', 2).as('b')).select(['a', 'b']) { it.value('name') }
    }

    @Override
    public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_lop_b__b_0created_29_c__c_out_jump2_cX_selectXnameX() {
        g.V.match('a',
                g.of().as('a').out('created').has('name', 'lop').as('b'),
                g.of().as('b').in('created').has('age', 29).as('c'),
                g.of().as('c').out().jump('c') { it.loops < 2 }).select { it.value('name') }
    }

    @Override
    public Traversal<Vertex, String> get_g_V_out_out_matchXa_0created_b__b_0knows_cX_selectXcX_outXcreatedX_name() {
        g.V.out.out.match('a',
                g.of().as('a').in('created').as('b'),
                g.of().as('b').in('knows').as('c')).select('c').out('created').name
    }

    @Override
    public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_knows_b__b_created_lop__b_matchXa1_created_b1__b1_0created_c1X_selectXc1X_cX_selectXnameX() {
        g.V.match('a',
                g.of().as('a').out('knows').as('b'),
                g.of().as('b').out('created').has('name', 'lop'),
                g.of().as('b').match('a1',
                        g.of().as('a1').out('created').as('b1'),
                        g.of().as('b1').in('created').as('c1')).select('c1').as('c')).select { it.value('name') }
    }

    /*@Override
    public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__c_created_bX_selectXnameX() {
        g.V.match("a",
                g.of().as("a").out('created').as("b"),
                g.of().as('c').out("created").as('b')).select { it.value('name') }
    }

    @Override
    public Traversal<Vertex, String> get_g_V_out_out_hasXname_rippleX_matchXb_created_a__c_knows_bX_selectXcX_outXknowsX_name() {
        g.V.out.out.match('a',
                g.of().as('b').out("created").as('a'),
                g.of().as('c').out("knows").as("b")).select('c').out('knows').name
    }*/

}