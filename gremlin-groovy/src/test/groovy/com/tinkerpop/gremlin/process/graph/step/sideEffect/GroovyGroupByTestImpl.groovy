package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyGroupByTestImpl extends GroupByTest {

    public Traversal<Vertex, Map<String, List<Vertex>>> get_g_V_groupByXnameX() {
        g.V.groupBy { it.value('name') }
    }

    public Map<String, List<String>> get_g_V_hasXlangX_groupByXa_lang_nameX_iterate_getXaX() {
        g.V.has('lang').groupBy('a') { it.value('name') }.iterate().variables().get('a')
    }

    public Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_groupByXlang_1_sizeX() {
        g.V.has('lang').groupBy { it.value('lang') } { 1 } { it.size() }
    }
}
