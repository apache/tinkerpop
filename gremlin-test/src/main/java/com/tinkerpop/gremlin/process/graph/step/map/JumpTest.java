package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    public abstract Traversal<Vertex, String> get_g_V_asXxX_out_jumpXx_loops_lt_2X_asXyX_in_jumpXy_loops_lt_2X_name();

    public abstract Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_2X();

    public abstract Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_2_trueX();

    public abstract Traversal<Vertex, Path> get_g_v1_out_jumpXx_t_out_hasNextX_in_jumpXyX_asXxX_out_asXyX_path(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_jumpXxX_out_out_asXxX();

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

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_asXxX_out_jumpXx_loops_lt_2X_asXyX_in_jumpXy_loops_lt_2X_name() {
        final Iterator<String> step = get_g_V_asXxX_out_jumpXx_loops_lt_2X_asXyX_in_jumpXy_loops_lt_2X_name();
        System.out.println("Testing: " + step);
        int count = 0;
        while (step.hasNext()) {
            assertEquals("marko", step.next());
            count++;
        }
        assertEquals(2, count);
        assertFalse(step.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_asXxX_out_jumpXx_2() {
        final Iterator<Vertex> step = get_g_V_asXxX_out_jumpXx_2X();
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
    public void g_V_asXxX_out_jumpXx_2_trueX() {
        final Iterator<Vertex> step = get_g_V_asXxX_out_jumpXx_2_trueX();
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
    public void g_v1_out_jumpXx_t_out_hasNextX_in_jumpXyX_asXxX_out_asXyX_path() {
        Iterator<Path> traversal = get_g_v1_out_jumpXx_t_out_hasNextX_in_jumpXyX_asXxX_out_asXyX_path(convertToVertexId("marko"));
        System.out.println("Testing: " + traversal);
        final List<Path> paths = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(6, paths.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_jumpXxX_out_out_asXxX() {
        Iterator<Vertex> traversal = get_g_V_jumpXxX_out_out_asXxX();
        System.out.println("Testing: " + traversal);
        assertTrue(traversal.hasNext());
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            vertices.add(traversal.next());
            counter++;
        }
        assertEquals(6, counter);
        assertEquals(6, vertices.size());
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

        public Traversal<Vertex, String> get_g_V_asXxX_out_jumpXx_loops_lt_2X_asXyX_in_jumpXy_loops_lt_2X_name() {
            return g.V().as("x").out().jump("x", t -> t.getLoops() < 2).as("y").in().jump("y", t -> t.getLoops() < 2).value("name");
        }

        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_2X() {
            return g.V().as("x").out().jump("x", 2);
        }

        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_2_trueX() {
            return g.V().as("x").out().jump("x", 2, t -> true);
        }

        public Traversal<Vertex, Path> get_g_v1_out_jumpXx_t_out_hasNextX_in_jumpXyX_asXxX_out_asXyX_path(final Object v1Id) {
            return g.v(v1Id).out().jump("x", t -> t.get().out().hasNext()).in().jump("y").as("x").out().as("y").path();
        }

        public Traversal<Vertex, Vertex> get_g_V_jumpXxX_out_out_asXxX() {
            return g.V().jump("x").out().out().as("x");
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

        public Traversal<Vertex, String> get_g_V_asXxX_out_jumpXx_loops_lt_2X_asXyX_in_jumpXy_loops_lt_2X_name() {
            return g.V().as("x").out().jump("x", t -> t.getLoops() < 2).as("y").in().jump("y", t -> t.getLoops() < 2).<String>value("name").submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_2X() {
            return g.V().as("x").out().jump("x", 2).submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_2_trueX() {
            return g.V().as("x").out().jump("x", 2, t -> true).submit(g.compute());
        }

        public Traversal<Vertex, Path> get_g_v1_out_jumpXx_t_out_hasNextX_in_jumpXyX_asXxX_out_asXyX_path(final Object v1Id) {
            return g.v(v1Id).out().jump("x", t -> t.get().out().hasNext()).in().jump("y").as("x").out().as("y").path().submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_V_jumpXxX_out_out_asXxX() {
            return g.V().jump("x").out().out().as("x").submit(g.compute());
        }
    }
}
