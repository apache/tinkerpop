package com.tinkerpop.gremlin.process.graph.traversal.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.traversal.__.out;
import static com.tinkerpop.gremlin.process.graph.traversal.__.outE;
import static org.junit.Assert.*;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class CoalesceTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_coalesceXoutXfooX_outXbarXX();

    public abstract Traversal<Vertex, String> get_g_VX1X_coalesceXoutXknowsX_outXcreatedXX_valuesXnameX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_coalesceXoutXcreatedX_outXknowsXX_valuesXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_coalesceXoutXlikesX_outXknowsX_inXcreatedXX_groupCount_byXnameX();

    public abstract Traversal<Vertex, Path> get_g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_coalesceXoutXfooX_outXbarXX() {
        Traversal<Vertex, Vertex> traversal = get_g_V_coalesceXoutXfooX_outXbarXX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_coalesceXoutXknowsX_outXcreatedXX_valuesXnameX() {
        Traversal<Vertex, String> traversal = get_g_VX1X_coalesceXoutXknowsX_outXcreatedXX_valuesXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh", "vadas"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_coalesceXoutXcreatedX_outXknowsXX_valuesXnameX() {
        Traversal<Vertex, String> traversal = get_g_VX1X_coalesceXoutXcreatedX_outXknowsXX_valuesXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals("lop", traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_coalesceXoutXlikesX_outXknowsX_inXcreatedXX_groupCount_byXnameX() {
        Traversal<Vertex, Map<String, Long>> traversal = get_g_V_coalesceXoutXlikesX_outXknowsX_inXcreatedXX_groupCount_byXnameX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        Map<String, Long> result = traversal.next();
        assertEquals(4, result.size());
        assertTrue(result.containsKey("josh") && result.containsKey("lop") && result.containsKey("ripple") && result.containsKey("vadas"));
        assertEquals(1L, (long) result.get("josh"));
        assertEquals(2L, (long) result.get("lop"));
        assertEquals(1L, (long) result.get("ripple"));
        assertEquals(1L, (long) result.get("vadas"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX() {
        Traversal<Vertex, Path> traversal = get_g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX();
        printTraversalForm(traversal);
        Map<String, Integer> first = new HashMap<>();
        Map<String, Integer> last = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Path path = traversal.next();
            first.compute(path.<String>get(0), (k, v) -> v != null ? v + 1 : 1);
            last.compute(path.<String>get(2), (k, v) -> v != null ? v + 1 : 1);
            assertEquals(3, path.size());
            assertTrue((path.<String>get(0).equals("marko") && path.<String>get(1).equals("knows") && path.<String>get(2).equals("vadas"))
                    || (path.<String>get(0).equals("marko") && path.<String>get(1).equals("knows") && path.<String>get(2).equals("josh"))
                    || (path.<String>get(0).equals("josh") && path.<String>get(1).equals("created") && path.<String>get(2).equals("lop"))
                    || (path.<String>get(0).equals("josh") && path.<String>get(1).equals("created") && path.<String>get(2).equals("ripple"))
                    || (path.<String>get(0).equals("peter") && path.<String>get(1).equals("created") && path.<String>get(2).equals("lop")));
        }
        assertEquals(5, counter);
        assertEquals(2, (int) first.get("marko"));
        assertEquals(2, (int) first.get("josh"));
        assertEquals(1, (int) first.get("peter"));
        assertEquals(1, (int) last.get("josh"));
        assertEquals(1, (int) last.get("vadas"));
        assertEquals(2, (int) last.get("lop"));
        assertEquals(1, (int) last.get("ripple"));
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends CoalesceTest {

        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coalesceXoutXfooX_outXbarXX() {
            return g.V().coalesce(out("foo"), out("bar"));
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_coalesceXoutXknowsX_outXcreatedXX_valuesXnameX(final Object v1Id) {
            return g.V(v1Id).coalesce(out("knows"), out("created")).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_coalesceXoutXcreatedX_outXknowsXX_valuesXnameX(final Object v1Id) {
            return g.V(v1Id).coalesce(out("created"), out("knows")).values("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_coalesceXoutXlikesX_outXknowsX_inXcreatedXX_groupCount_byXnameX() {
            return g.V().coalesce(out("likes"), out("knows"), out("created")).groupCount().by("name").cap();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX() {
            return g.V().coalesce(outE("knows"), outE("created")).otherV().path().by("name").by(T.label);
        }
    }

    public static class ComputerTest extends StandardTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coalesceXoutXfooX_outXbarXX() {
            return super.get_g_V_coalesceXoutXfooX_outXbarXX().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_coalesceXoutXknowsX_outXcreatedXX_valuesXnameX(final Object v1Id) {
            return super.get_g_VX1X_coalesceXoutXknowsX_outXcreatedXX_valuesXnameX(v1Id).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_coalesceXoutXcreatedX_outXknowsXX_valuesXnameX(final Object v1Id) {
            return super.get_g_VX1X_coalesceXoutXcreatedX_outXknowsXX_valuesXnameX(v1Id).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_coalesceXoutXlikesX_outXknowsX_inXcreatedXX_groupCount_byXnameX() {
            return super.get_g_V_coalesceXoutXlikesX_outXknowsX_inXcreatedXX_groupCount_byXnameX().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX() {
            return super.get_g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX().submit(g.compute());
        }
    }
}
