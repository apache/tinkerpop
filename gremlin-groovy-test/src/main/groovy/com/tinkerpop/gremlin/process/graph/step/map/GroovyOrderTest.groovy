package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Order
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
        public Traversal<Vertex, String> get_g_V_name_order_byXabX() {
            g.V.name.order.by { a, b -> b <=> a }
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_order_byXa1_b1X_byXb2_a2X() {
            g.V.name.order.by { a, b -> a[1] <=> b[1] }.by { a, b -> b[2] <=> a[2] }
        }

        @Override
        public Traversal<Vertex, String> get_g_V_order_byXname_incrX_name() {
            g.V.order.by('name', Order.incr).name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_order_byXnameX_name() {
            g.V.order.by('name', Order.incr).name
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_outE_order_byXweight_decrX_weight() {
            g.V.outE.order.by('weight', Order.decr).weight
        }

        @Override
        public Traversal<Vertex, String> get_g_V_order_byXname_a1_b1X_byXname_b2_a2X_name() {
            return g.V.order.by('name', { a, b -> a[1].compareTo(b[1]) }).by('name', { a, b -> b[2].compareTo(a[2]) }).name;
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_asXaX_outXcreatedX_asXbX_order_byXshuffleX_select() {
            g.V.as('a').out('created').as('b').order.by(Order.shuffle).select();
        }
    }

    public static class ComputerTest extends OrderTest {

        @Override
        public Traversal<Vertex, String> get_g_V_name_order() {
            ComputerTestHelper.compute("g.V().name.order()", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_order_byXabX() {
            ComputerTestHelper.compute("g.V.name.order.by { a, b -> b <=> a }", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_order_byXa1_b1X_byXb2_a2X() {
            ComputerTestHelper.compute("g.V.name.order.by { a, b -> a[1] <=> b[1] }.by{ a, b -> b[2] <=> a[2] }", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_order_byXname_incrX_name() {
            g.V.order.by('name', Order.incr).name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_order_byXnameX_name() {
            g.V.order.by('name', Order.incr).name
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_outE_order_byXweight_decrX_weight() {
            g.V.outE.order.by('weight', Order.decr).weight
        }

        @Override
        public Traversal<Vertex, String> get_g_V_order_byXname_a1_b1X_byXname_b2_a2X_name() {
            return g.V.order.by('name') { a, b -> a[1].compareTo(b[1]) }.by('name') { a, b -> b[2].compareTo(a[2]) }.name;
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_asXaX_outXcreatedX_asXbX_order_byXshuffleX_select() {
            ComputerTestHelper.compute("g.V.as('a').out('created').as('b').order.by(Order.shuffle).select()", g);
        }

    }
}
