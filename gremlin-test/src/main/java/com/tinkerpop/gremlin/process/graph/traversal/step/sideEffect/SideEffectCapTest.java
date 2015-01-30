package com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.traversal.__.*;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class SideEffectCapTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX();

    public abstract Traversal<Vertex, Map<String, Map<Object, Long>>> get_g_V_chooseXlabel_person__age_groupCountXaX__name_groupCountXbXX_capXa_bX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX();
        printTraversalForm(traversal);
        Map<String, Long> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(map.get("marko"), new Long(1l));
        assertEquals(map.get("vadas"), new Long(1l));
        assertEquals(map.get("peter"), new Long(1l));
        assertEquals(map.get("josh"), new Long(1l));
        assertEquals(map.size(), 4);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXlabel_person__age_groupCountXaX__name_groupCountXbXX_capXa_bX() {
        final Traversal<Vertex, Map<String, Map<Object, Long>>> traversal = get_g_V_chooseXlabel_person__age_groupCountXaX__name_groupCountXbXX_capXa_bX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        Map<String, Map<Object, Long>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertTrue(map.containsKey("a"));
        assertTrue(map.containsKey("b"));
        assertEquals(4, map.get("a").size());
        assertEquals(2, map.get("b").size());
        assertEquals(1l, map.get("a").get(27).longValue());
        assertEquals(1l, map.get("a").get(29).longValue());
        assertEquals(1l, map.get("a").get(32).longValue());
        assertEquals(1l, map.get("a").get(35).longValue());
        assertEquals(1l, map.get("b").get("ripple").longValue());
        assertEquals(1l, map.get("b").get("lop").longValue());
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends SideEffectCapTest {
        public StandardTest() {
            this.requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX() {
            return g.V().has("age").groupCount("a").by("name").out().cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Map<Object, Long>>> get_g_V_chooseXlabel_person__age_groupCountXaX__name_groupCountXbXX_capXa_bX() {
            return g.V().choose(has(T.label, "person"),
                    values("age").groupCount("a"),
                    values("name").groupCount("b")).cap("a", "b");
        }
    }

    public static class ComputerTest extends SideEffectCapTest {

        public ComputerTest() {
            this.requiresGraphComputer = true;
        }


        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX() {
            return g.V().has("age").groupCount("a").by("name").out().<Map<String, Long>>cap("a").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Map<Object, Long>>> get_g_V_chooseXlabel_person__age_groupCountXaX__name_groupCountXbXX_capXa_bX() {
            return g.V().choose(has(T.label, "person"),
                    values("age").groupCount("a"),
                    values("name").groupCount("b")).<Map<String, Map<Object, Long>>>cap("a", "b").submit(g.compute());
        }
    }
}
