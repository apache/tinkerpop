package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Order;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__;
import static org.junit.Assert.*;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class LocalTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value();

    public abstract Traversal<Vertex, Long> get_g_V_localXoutE_countX();

    public abstract Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlabel_personX_localXoutXcreatedX_group_byXlangX_byXnameX();

    //public abstract Traversal<Vertex, Map<Double, Long>> get_g_V_localXoutE_weight_groupCountX();


    @Test
    @LoadGraphWith(CREW)
    public void g_V_localXpropertiesXlocationX_orderByXvalueX_limitX2XX_value() {
        final Traversal<Vertex, String> traversal = get_g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("brussels", "san diego", "centreville", "dulles", "baltimore", "bremen", "aachen", "kaiserslautern"), traversal);

    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_localXoutE_countX() {
        final Traversal<Vertex, Long> traversal = get_g_V_localXoutE_countX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(3l, 0l, 0l, 0l, 1l, 2l), traversal);

    }

    @Test
    @Ignore("Do local sideEffect reset?")
    @LoadGraphWith(MODERN)
    public void g_V_hasXlabel_personX_localXoutXcreatedX_group_byXlangX_byXnameX() {
        final Traversal<Vertex, Map<String, Collection<String>>> traversal = get_g_V_hasXlabel_personX_localXoutXcreatedX_group_byXlangX_byXnameX();
        printTraversalForm(traversal);
        int counter = 0;
        int count0 = 0;
        int count1 = 0;
        int count2 = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Collection<String>> map = traversal.next();
            if (map.size() == 0) {
                count0++;
            } else if (map.size() == 1) {
                if (map.get("java").size() == 1) {
                    count1++;
                    assertEquals(1, map.get("java").size());
                    assertTrue(map.get("java").contains("lop"));
                } else if (map.get("java").size() == 2) {
                    count2++;
                    assertEquals(2, map.get("java").size());
                    assertTrue(map.get("java").contains("lop"));
                    assertTrue(map.get("java").contains("ripple"));
                }
            } else {
                fail("No suppose to be a map this big: " + map.size());
            }
        }
        assertFalse(traversal.hasNext());
        assertEquals(4, counter);
        assertEquals(1, count0);
        assertEquals(2, count1);
        assertEquals(1, count2);
    }

    /*@Test
    @LoadGraphWith(MODERN)
    public void g_V_localXoutE_weight_groupCountX() {
        final Traversal<Vertex, Map<Double, Long>> traversal = get_g_V_localXoutE_weight_groupCountX();
        int counter = 0;
        int zeroCounter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<Double, Long> map = traversal.next();
            if(0 == map.size()) zeroCounter++;
            System.out.println(map);
        }
        assertEquals(6, counter);
        assertEquals(3, zeroCounter);
    }*/


    public static class StandardTest extends LocalTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value() {
            return g.V().local(__.properties("location").order().by(T.value, Order.incr).range(0, 2)).value();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_localXoutE_countX() {
            return g.V().local(__.outE().count());
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlabel_personX_localXoutXcreatedX_group_byXlangX_byXnameX() {
            return (Traversal) g.V().has(T.label, "person").local(__.out("created").group().by("lang").by("name").cap());
        }

        /*@Override
        public Traversal<Vertex, Map<Double, Long>> get_g_V_localXoutE_weight_groupCountX() {
            return g.V().local((Traversal) __.outE().values("weight").groupCount());
        }*/

    }

    public static class ComputerTest extends LocalTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value() {
            return g.V().local(__.properties("location").order().by(T.value, Order.incr).range(0, 2)).<String>value().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_localXoutE_countX() {
            return g.V().local(__.outE().count()).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlabel_personX_localXoutXcreatedX_group_byXlangX_byXnameX() {
            return (Traversal) g.V().has(T.label, "person").local(__.out("created").group().by("lang").by("name")).submit(g.compute());
        }

        /*@Override
        public Traversal<Vertex, Map<Double, Long>> get_g_V_localXoutE_weight_groupCountX() {
            return g.V().local((Traversal) __.outE().values("weight").groupCount()).submit(g.compute());
        }*/

    }
}
