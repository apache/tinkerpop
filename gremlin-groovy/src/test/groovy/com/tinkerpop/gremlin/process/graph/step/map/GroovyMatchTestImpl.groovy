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
    }

}