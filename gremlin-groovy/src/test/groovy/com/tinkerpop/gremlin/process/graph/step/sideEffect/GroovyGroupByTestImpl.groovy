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

    public Traversal<Vertex, Map<String, List<String>>> get_g_V_hasXlangX_groupByXlang_nameX_asXaX_out_capXaX() {
        g.V.has('lang').groupBy{ it.value('lang') } { it.value('name') }.as('a').out.cap('a')
    }

    public Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_groupByXlang_1_sizeX() {
        g.V.has('lang').groupBy { it.value('lang') } { 1 } { it.size() }
    }

    public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_2X_capXaX() {
        g.V().as("x").out().groupBy{ it.value('name') } { it } { it.size() }.as('a').jump("x", 2).cap("a");
    }

    public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_loops_lt_2X_capXaX() {
        g.V().as("x").out().groupBy{ it.value('name') } { it } { it.size() }.as('a').jump("x") { it.loops < 2 }.cap("a");
    }
}
