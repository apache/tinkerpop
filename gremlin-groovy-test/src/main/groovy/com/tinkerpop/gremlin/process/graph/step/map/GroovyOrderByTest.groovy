package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Order
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyOrderByTest {

    public static class StandardTest extends OrderByTest {

        @Override
        public Traversal<Vertex, String> get_g_V_orderByXname_incrX_name() {
            g.V.orderBy('name', Order.incr).name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_orderByXnameX_name() {
            g.V.orderBy('name').name
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_outE_orderByXweight_decrX_weight() {
            g.V.outE.orderBy('weight', Order.decr).weight
        }

        @Override
        public Traversal<Vertex, String> get_g_V_orderByXname_a1_b1__b2_a2X_name() {
            return g.V.orderBy('name', { a, b -> a[1].compareTo(b[1]) }, 'name', { a, b -> b[2].compareTo(a[2]) }).name;
        }
    }
}
