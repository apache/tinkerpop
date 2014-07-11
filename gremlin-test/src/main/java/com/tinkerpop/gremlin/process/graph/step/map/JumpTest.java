package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class JumpTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_loops_lt_2X();

    public abstract Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX();

    public abstract Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX_path();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX() {
        final Iterator<String> step = get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(convertToVertexId("marko"));
        System.out.println("Testing: " + step);
        List<String> names = new ArrayList<>();
        while (step.hasNext()) {
            names.add(step.next());
        }
        assertEquals(2, names.size());
        assertTrue(names.contains("ripple"));
        assertTrue(names.contains("lop"));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_asXxX_out_jumpXx_loops_lt_2X() {
        final Iterator<Vertex> step = get_g_V_asXxX_out_jumpXx_loops_lt_2X();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            Vertex vertex = step.next();
            assertTrue(vertex.value("name").equals("lop") || vertex.value("name").equals("ripple"));
        }
        assertEquals(2, counter);
        assertFalse(step.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_asXxX_out_jumpXx_loops_lt_2_trueX() {
        final Iterator<Vertex> step = get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX();
        System.out.println("Testing: " + step);
        Map<String, Long> map = new HashMap<>();
        while (step.hasNext()) {
            Vertex vertex = step.next();
            MapHelper.incr(map, vertex.value("name"), 1l);
        }
        assertEquals(4, map.size());
        assertTrue(map.containsKey("vadas"));
        assertTrue(map.containsKey("josh"));
        assertTrue(map.containsKey("ripple"));
        assertTrue(map.containsKey("lop"));
        assertEquals(new Long(1), map.get("vadas"));
        assertEquals(new Long(1), map.get("josh"));
        assertEquals(new Long(2), map.get("ripple"));
        assertEquals(new Long(4), map.get("lop"));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_asXxX_out_jumpXx_loops_lt_2_trueX_path() {
        final Iterator<Path> step = get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX_path();
        System.out.println("Testing: " + step);
        final Map<Integer, Long> pathLengths = new HashMap<>();
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            MapHelper.incr(pathLengths, step.next().size(), 1l);
        }
        assertEquals(2, pathLengths.size());
        assertEquals(8, counter);
        assertEquals(new Long(6), pathLengths.get(2));
        assertEquals(new Long(2), pathLengths.get(3));
    }


    public static class JavaJumpTest extends JumpTest {
        public JavaJumpTest() {
            requiresGraphComputer = false;
        }

        public Traversal<Vertex, String> get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(final Object v1Id) {
            return g.v(v1Id).as("x").out().jump("x", h -> h.getLoops() < 2).value("name");
        }

        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_loops_lt_2X() {
            return g.V().as("x").out().jump("x", t -> t.getLoops() < 2);
        }

        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX() {
            return g.V().as("x").out().jump("x", t -> t.getLoops() < 2, t -> true);
        }

        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX_path() {
            return g.V().as("x").out().jump("x", t -> t.getLoops() < 2, t -> true).path();
        }
    }

    public static class JavaComputerJumpTest extends JumpTest {
        public JavaComputerJumpTest() {
            requiresGraphComputer = true;
        }

        public Traversal<Vertex, String> get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(final Object v1Id) {
            return g.v(v1Id).as("x").out().jump("x", t -> t.getLoops() < 2).<String>value("name").submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_loops_lt_2X() {
            return g.V().as("x").out().jump("x", t -> t.getLoops() < 2).submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX() {
            return g.V().as("x").out().jump("x", t -> t.getLoops() < 2, t -> true).submit(g.compute());
        }

        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX_path() {
            return g.V().as("x").out().jump("x", t -> t.getLoops() < 2, t -> true).path().submit(g.compute());
        }
    }
}
