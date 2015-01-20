package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyUnfoldTest {

    public static class StandardTest extends UnfoldTest {

        @Override
        public Traversal<Vertex, Edge> get_g_V_mapXoutEX_unfold() {
            g.V.map { it.outE }.unfold
        }

        @Override
        public Traversal<Vertex, String> get_V_valueMap_unfold_mapXkeyX() {
            g.V.valueMap.unfold.map { it.key }
        }
    }

    public static class ComputerTest extends UnfoldTest {

        @Override
        public Traversal<Vertex, Edge> get_g_V_mapXoutEX_unfold() {
            ComputerTestHelper.compute("g.V.map { it.outE }.unfold", g)
        }

        @Override
        public Traversal<Vertex, String> get_V_valueMap_unfold_mapXkeyX() {
            ComputerTestHelper.compute("g.V.valueMap.unfold.map { it.key }", g)
        }
    }


}
