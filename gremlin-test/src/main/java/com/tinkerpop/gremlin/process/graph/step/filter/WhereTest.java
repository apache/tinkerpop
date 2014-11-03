package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class WhereTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_b_neqX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            assertTrue(map.containsKey("a"));
            assertTrue(map.containsKey("b"));
            assertEquals(map.get("a"), map.get("b"));
        }
        assertEquals(6, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_b_neqX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_b_neqX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            assertTrue(map.containsKey("a"));
            assertTrue(map.containsKey("b"));
            assertNotEquals(map.get("a"), map.get("b"));
            assertTrue(((Vertex) map.get("a")).id().equals(convertToVertexId("marko")) ||
                    ((Vertex) map.get("a")).id().equals(convertToVertexId("peter")) ||
                    ((Vertex) map.get("a")).id().equals(convertToVertexId("josh")));
            assertTrue(((Vertex) map.get("b")).id().equals(convertToVertexId("marko")) ||
                    ((Vertex) map.get("b")).id().equals(convertToVertexId("peter")) ||
                    ((Vertex) map.get("b")).id().equals(convertToVertexId("josh")));
        }
        assertEquals(6, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX();
        printTraversalForm(traversal);
        int counter = 0;
        int markoCounter = 0;
        while (traversal.hasNext()) {
            counter++;
            Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            assertTrue(map.containsKey("a"));
            assertTrue(map.containsKey("b"));
            assertEquals(convertToVertexId("marko"), ((Vertex) map.get("b")).id());
            if (((Vertex) map.get("a")).id().equals(convertToVertexId("marko")))
                markoCounter++;
            else
                assertTrue(((Vertex) map.get("a")).id().equals(convertToVertexId("josh")) || ((Vertex) map.get("a")).id().equals(convertToVertexId("peter")));
        }
        assertEquals(3, markoCounter);
        assertEquals(5, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Map<String, Object> map = traversal.next();
            assertEquals(2, map.size());
            assertTrue(map.containsKey("a"));
            assertTrue(map.containsKey("b"));
            assertEquals(convertToVertexId("marko"), ((Vertex) map.get("a")).id());
            assertEquals(convertToVertexId("josh"), ((Vertex) map.get("b")).id());
        }
        assertEquals(1, counter);
        assertFalse(traversal.hasNext());
    }


    public static class StandardTest extends WhereTest {

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().where("a", Compare.eq, "b");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_b_neqX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().where("a", "b", (a, b) -> !a.equals(b));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().where(g.of().as("b").has("name", "marko"));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().where(g.of().as("a").out("knows").as("b"));
        }


    }

    public static class ComputerTest extends WhereTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().where("a", Compare.eq, "b").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_b_neqX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().where("a", "b", (a, b) -> !a.equals(b)).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().where(g.of().as("b").has("name", "marko")); // TODO: internal traversals
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().where(g.of().as("a").out("knows").as("b")); // TODO: internal traversals
        }
    }
}