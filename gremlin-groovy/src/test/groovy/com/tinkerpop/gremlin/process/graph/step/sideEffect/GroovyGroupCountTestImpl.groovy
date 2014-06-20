package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyGroupCountTestImpl extends GroupCountTest {

    public Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_groupCountXnameX() {
        g.V.out('created').groupCount { it['name'] }
    }

    public Traversal<Vertex, Map<Object, Long>> get_g_V_outXcreatedX_name_groupCount() {
        g.V.out('created').name.groupCount
    }

    public Map<Object, Long> get_g_V_asXxX_out_groupCountXa_nameX_jumpXx_loops_lt_2X_iterate_getXaX() {
        g.V.as('x').out.groupCount('a') { it['name'] }.jump('x') { it.loops < 2 }.iterate().memory().get('a')
    }
}
