package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyOrderTest {


    public static class StandardTest extends OrderTest {

        @Override
        public Traversal<Vertex, String> get_g_V_name_order() {
            g.V().name.order()
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_orderXabX() {
            g.V.name.order { a, b -> b.get() <=> a.get() }
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_orderXa1_b1__b2_a2X() {
            g.V.name.order { a, b -> a.get()[1] <=> b.get()[1] } { a, b -> b.get()[2] <=> a.get()[2] }
        }
    }

    public static class ComputerTest extends OrderTest {

        @Override
        public Traversal<Vertex, String> get_g_V_name_order() {
            ComputerTestHelper.compute("g.V().name.order()", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_orderXabX() {
            ComputerTestHelper.compute("g.V.name.order { a, b -> b.get() <=> a.get() }", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_orderXa1_b1__b2_a2X() {
            ComputerTestHelper.compute("g.V.name.order { a, b -> a.get()[1] <=> b.get()[1] } { a, b -> b.get()[2] <=> a.get()[2] }", g)
        }

    }
}
