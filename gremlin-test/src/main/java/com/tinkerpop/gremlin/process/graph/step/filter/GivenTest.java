package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC_DOUBLE;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GivenTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_givenXa_eq_bX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_givenXa_b_neqX();


    @Test
    @LoadGraphWith(CLASSIC_DOUBLE)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_givenXa_eq_bX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_givenXa_eq_bX();
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
    @LoadGraphWith(CLASSIC_DOUBLE)
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_givenXa_b_neqX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_givenXa_b_neqX();
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


    public static class JavaGivenTest extends GivenTest {

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_givenXa_eq_bX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().given("a", T.eq, "b");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_givenXa_b_neqX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().given("a", "b", (a, b) -> !a.equals(b));
        }


    }

    public static class JavaComputerGivenTest extends GivenTest {

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_givenXa_eq_bX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().given("a", T.eq, "b").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_givenXa_b_neqX() {
            return g.V().has("age").as("a").out().in().has("age").as("b").select().given("a", "b", (a, b) -> !a.equals(b)).submit(g.compute());
        }
    }
}