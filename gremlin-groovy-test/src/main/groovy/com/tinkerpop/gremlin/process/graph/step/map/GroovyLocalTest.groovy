package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Order
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyLocalTest {

    public static class StandardTest extends LocalTest {

        @Override
        public Traversal<Vertex, String> get_g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value() {
            g.V.local(__.properties('location').order.by(T.value, Order.incr).limit(2)).value
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_localXoutE_countX() {
            g.V.local(__.outE.count());
        }

        /*@Override
        public Traversal<Vertex, Map<Double, Long>> get_g_V_localXoutE_weight_groupCountX() {
            return g.V().local((Traversal) __.outE().values("weight").groupCount());
        }*/
    }

    public static class ComputerTest extends LocalTest {

        @Override
        public Traversal<Vertex, String> get_g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value() {
            ComputerTestHelper.compute("g.V.local(__.properties('location').order.by(T.value,Order.incr).limit(2)).value", g);
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_localXoutE_countX() {
            ComputerTestHelper.compute("g.V.local(__.outE.count())", g);
        }

        /*@Override
        public Traversal<Vertex, Map<Double, Long>> get_g_V_localXoutE_weight_groupCountX() {
            return g.V().local((Traversal) __.outE().values("weight").groupCount());
        }*/

    }

}
