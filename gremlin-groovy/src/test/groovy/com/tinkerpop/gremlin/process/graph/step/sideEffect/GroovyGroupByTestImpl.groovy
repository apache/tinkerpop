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

    public Traversal<Vertex, Map<String, List<String>>> get_g_V_hasXlangX_groupByXa_lang_nameX_out_capXaX() {
        g.V.has('lang').groupBy('a') { it.value('lang') } { it.value('name') }.out.cap('a')
    }

    public Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_groupByXlang_1_sizeX() {
        g.V.has('lang').groupBy { it.value('lang') } { 1 } { it.size() }
    }

    public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXa_name_sizeX_jumpXx_2X_capXaX() {
        g.V().as("x").out().groupBy('a') { it.value('name') } { it } { it.size() }.jump("x", 2).cap("a");
    }

    public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXa_name_sizeX_jumpXx_loops_lt_2X_capXaX() {
        g.V().as("x").out().groupBy('a') { it.value('name') } { it } { it.size() }.jump("x") { it.loops < 2 }.cap("a");
    }
}
