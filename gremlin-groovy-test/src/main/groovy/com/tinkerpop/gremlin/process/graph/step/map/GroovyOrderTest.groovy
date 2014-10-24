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

        /*@Override
        public Traversal<Vertex, String> get_g_V_orderXa_nameXb_nameX() {
            g.V.order { a, b -> a.name <=> b.name }
        }*/
    }

    public static class ComputerTest extends OrderTest {

        @Override
        public Traversal<Vertex, String> get_g_V_name_order() {
            ComputerTestHelper.compute("g.V().name.order()", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_orderXabX() {
            ComputerTestHelper.compute("g.V.name.order { a, b -> b.get() <=> a.get() }", g);
        }

       /* @Override
        public Traversal<Vertex, String> get_g_V_orderXa_nameXb_nameX() {
            ComputerTestHelper.compute("g.V.order { a, b -> a.name <=> b.name }", g);
        }*/

    }
}
