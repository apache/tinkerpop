package com.tinkerpop.gremlin.process.graph.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyGroupByTestG extends GroupByTest {

    public Traversal<Vertex, Map<String, List<Vertex>>> get_g_V_groupByXnameX() {
        g.V.groupBy{it['name']}
    }

    public Map<String, List<String>> get_g_V_hasXlangX_groupByXa_lang_nameX_iterate_getXaX() {
       g.V.has('lang').groupBy('a'){it['name']}.iterate().memory().get('a')
    }

    public Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_groupByXlang_1_sizeX(){
        g.V.has('lang').groupBy{it['lang']}{1}{it.size()}
    }
}
