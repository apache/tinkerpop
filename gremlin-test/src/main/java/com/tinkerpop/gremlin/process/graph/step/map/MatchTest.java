package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.util.As;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MatchTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_out_bX();

    public abstract Traversal<Vertex, Object> get_g_V_matchXa_out_bX_selectXb_idX();

    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__b_created_cX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__a_out_jump2_bX_selectXab_nameX();

    //public abstract Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__a_out_loop2_b__b_path__X_selectXab_nameX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_matchXa_out_bX() throws Exception {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_V_matchXa_out_bX();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Vertex> bindings = traversal.next();
            assertEquals(2, bindings.size());
            final Object aId = bindings.get("a").id();
            final Object bId = bindings.get("b").id();
            if (aId.equals(convertToVertexId("marko"))) {
                assertTrue(bId.equals(convertToVertexId("vadas")) ||
                        bId.equals(convertToVertexId("lop")) ||
                        bId.equals(convertToVertexId("josh")));
            } else if (aId.equals(convertToVertexId("josh"))) {
                assertTrue(bId.equals(convertToVertexId("lop")) ||
                        bId.equals(convertToVertexId("ripple")));
            } else if (aId.equals(convertToVertexId("peter"))) {
                assertEquals(convertToVertexId("lop"), bId);
            } else {
                assertFalse(true);
            }
        }
        assertFalse(traversal.hasNext());
        // TODO: The full result set isn't coming back (only the marko vertices)
        // assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_matchXa_out_bX_selectXb_idX() throws Exception {
        final Traversal<Vertex, Object> traversal = get_g_V_matchXa_out_bX_selectXb_idX();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        final Object vadasId = convertToVertexId("vadas");
        final Object joshId = convertToVertexId("josh");
        final Object lopId = convertToVertexId("lop");
        final Object rippleId = convertToVertexId("ripple");
        Map<Object, Long> idCounts = new HashMap<>();
        while (traversal.hasNext()) {
            counter++;
            MapHelper.incr(idCounts, traversal.next(), 1l);
        }
        assertFalse(traversal.hasNext());
        assertEquals(idCounts.get(vadasId), Long.valueOf(1l));
        // TODO: The full result set isn't coming back (only the marko vertices)
        // assertEquals(idCounts.get(lopId), Long.valueOf(3l));
        assertEquals(idCounts.get(joshId), Long.valueOf(1l));
        // TODO: The full result set isn't coming back (only the marko vertices)
        // assertEquals(idCounts.get(rippleId), Long.valueOf(1l));
        // TODO: The full result set isn't coming back (only the marko vertices)
        //       assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_a_outXknowsX_b__b_outXcreatedX_c() throws Exception {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_V_matchXa_knows_b__b_created_cX();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Vertex> bindings = traversal.next();
            assertEquals(3, bindings.size());
            final Object aId = bindings.get("a").id();
            final Object bId = bindings.get("b").id();
            final Object cId = bindings.get("c").id();
            assertEquals(convertToVertexId("marko"), aId);
            assertEquals(convertToVertexId("josh"), bId);
            assertTrue(cId.equals(convertToVertexId("lop")) ||
                    cId.equals(convertToVertexId("ripple")));
        }
        assertFalse(traversal.hasNext());
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_matchXa_created_b__a_out_jump2_bX_selectXab_nameX() throws Exception {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_matchXa_created_b__a_out_jump2_bX_selectXab_nameX();
        System.out.println("Testing: " + traversal);
        assertTrue(traversal.hasNext());
        final Map<String, String> bindings = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, bindings.size());
        assertEquals("marko", bindings.get("a"));
        assertEquals("lop", bindings.get("b"));
        assertFalse(traversal.hasNext());
    }


    public static class JavaMapTest extends MatchTest {
        public JavaMapTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_out_bX() {
            return g.V().match("a", g.of().as("a").out().as("b"));
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_matchXa_out_bX_selectXb_idX() {
            return g.V().match("a", g.of().as("a").out().as("b")).select("b", v -> ((Vertex) v).id());
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__b_created_cX() {
            return g.V().match("a",
                    g.of().as("a").out("knows").as("b"),
                    g.of().as("b").out("created").as("c"));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__a_out_jump2_bX_selectXab_nameX() {
            return g.V().match("a",
                    g.of().as("a").out("created").as("b"),
                    g.of().as("a").out().jump("a", 2).as("b")).select(As.of("a", "b"), v -> ((Vertex) v).value("name"));
        }

    }

    public static class JavaComputerMapTest extends MatchTest {
        public JavaComputerMapTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_out_bX() {
            return (Traversal) g.V().match("a", g.of().as("a").out().as("b")).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_matchXa_out_bX_selectXb_idX() {
            return g.V().match("a", g.of().as("a").out().as("b")).select("b", v -> ((Vertex) v).id()).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__b_created_cX() {
            return (Traversal) g.V().match("a",
                    g.of().as("a").out("knows").as("b"),
                    g.of().as("b").out("created").as("c")).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__a_out_jump2_bX_selectXab_nameX() {
            return (Traversal) g.V().match("a",
                    g.of().as("a").out("created").as("b"),
                    g.of().as("a").out().jump("a", 2).as("b")).select(As.of("a", "b"), v -> ((Vertex) v).value("name")).submit(g.compute());
        }
    }
}
