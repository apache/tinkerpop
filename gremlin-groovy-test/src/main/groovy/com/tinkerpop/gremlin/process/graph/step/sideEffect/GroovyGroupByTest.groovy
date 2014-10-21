package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyGroupByTest {

    public static class StandardTest extends GroupByTest {

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_groupByXnameX() {
            g.V.groupBy { it.name }
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlangX_groupByXa_lang_nameX_out_capXaX() {
            g.V.has('lang').groupBy('a') { it.lang } { it.name }.out.cap('a')
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_groupByXlang_1_sizeX() {
            g.V.has('lang').groupBy { it.lang } { 1 } { it.size() }
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXa_name_sizeX_jumpXx_2X_capXaX() {
            g.V.as("x").out.groupBy('a') { it.name } { it.get() } { it.size() }.jump("x", 2).cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXa_name_sizeX_jumpXx_loops_lt_2X_capXaX() {
            g.V.as("x").out().groupBy('a') { it.name } { it.get() } { it.size() }.jump("x") { it.loops() < 2 }.cap("a");
        }
    }

    public static class ComputerTest extends GroupByTest {

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_groupByXnameX() {
            ComputerTestHelper.compute("g.V.groupBy { it.name }", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlangX_groupByXa_lang_nameX_out_capXaX() {
            ComputerTestHelper.compute("g.V.has('lang').groupBy('a') { it.lang } { it.name }.out.cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_groupByXlang_1_sizeX() {
            ComputerTestHelper.compute("g.V.has('lang').groupBy { it.lang } { 1 } { it.size() }", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXa_name_sizeX_jumpXx_2X_capXaX() {
            ComputerTestHelper.compute("g.V.as('x').out.groupBy('a') { it.name } { it.get() } { it.size() }.jump('x', 2).cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXa_name_sizeX_jumpXx_loops_lt_2X_capXaX() {
            ComputerTestHelper.compute("g.V.as('x').out().groupBy('a') { it.name } { it.get() } { it.size() }.jump('x') { it.loops() < 2 }.cap('a')", g)
        }
    }
}
