package com.tinkerpop.gremlin.process.graph.step.branch;

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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
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

    public abstract Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_2_trueX_path();

    public abstract Traversal<Vertex, String> get_g_V_asXxX_out_jumpXx_loops_lt_2X_asXyX_in_jumpXy_loops_lt_2X_name();

    public abstract Traversal<Vertex, String> get_g_V_asXxX_out_jumpXx_2X_asXyX_in_jumpXy_2X_name();

    public abstract Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_2X();

    public abstract Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_2_trueX();

    public abstract Traversal<Vertex, Path> get_g_v1_out_jumpXx_t_out_hasNextX_in_jumpXyX_asXxX_out_asXyX_path(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_jumpXxX_out_out_asXxX();

    public abstract Traversal<Vertex, String> get_g_v1_asXaX_jumpXb_loops_gt_1X_out_jumpXaX_asXbX_name(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX() {
        final List<Traversal<Vertex, String>> traversals = new ArrayList<>();
        traversals.add(get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(convertToVertexId("marko")));
        traversals.add(get_g_v1_asXaX_jumpXb_loops_gt_1X_out_jumpXaX_asXbX_name(convertToVertexId("marko")));
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            List<String> names = new ArrayList<>();
            while (traversal.hasNext()) {
                names.add(traversal.next());
            }
            assertEquals(2, names.size());
            assertTrue(names.contains("ripple"));
            assertTrue(names.contains("lop"));
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXxX_out_jumpXx_loops_lt_2_trueX_path() {
        final List<Traversal<Vertex, Path>> traversals = new ArrayList<>();
        traversals.add(get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX_path());
        traversals.add(get_g_V_asXxX_out_jumpXx_2_trueX_path());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Map<Integer, Long> pathLengths = new HashMap<>();
            int counter = 0;
            while (traversal.hasNext()) {
                counter++;
                MapHelper.incr(pathLengths, traversal.next().size(), 1l);
            }
            assertEquals(2, pathLengths.size());
            assertEquals(8, counter);
            assertEquals(new Long(6), pathLengths.get(2));
            assertEquals(new Long(2), pathLengths.get(3));
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXxX_out_jumpXx_2X_asXyX_in_jumpXy_2X_name() {
        final List<Traversal<Vertex, String>> traversals = new ArrayList<>();
        traversals.add(get_g_V_asXxX_out_jumpXx_loops_lt_2X_asXyX_in_jumpXy_loops_lt_2X_name());
        traversals.add(get_g_V_asXxX_out_jumpXx_2X_asXyX_in_jumpXy_2X_name());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            int count = 0;
            while (traversal.hasNext()) {
                assertEquals("marko", traversal.next());
                count++;
            }
            assertEquals(2, count);
            assertFalse(traversal.hasNext());
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXxX_out_jumpXx_2() {
        final List<Traversal<Vertex, Vertex>> traversals = new ArrayList<>();
        traversals.add(get_g_V_asXxX_out_jumpXx_2X());
        traversals.add(get_g_V_asXxX_out_jumpXx_loops_lt_2X());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            int counter = 0;
            while (traversal.hasNext()) {
                counter++;
                Vertex vertex = traversal.next();
                assertTrue(vertex.value("name").equals("lop") || vertex.value("name").equals("ripple"));
            }
            assertEquals(2, counter);
            assertFalse(traversal.hasNext());
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXxX_out_jumpXx_2_trueX() {
        final List<Traversal<Vertex, Vertex>> traversals = new ArrayList<>();
        traversals.add(get_g_V_asXxX_out_jumpXx_2_trueX());
        traversals.add(get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            Map<String, Long> map = new HashMap<>();
            while (traversal.hasNext()) {
                Vertex vertex = traversal.next();
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
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_out_jumpXx_t_out_hasNextX_in_jumpXyX_asXxX_out_asXyX_path() {
        Traversal<Vertex, Path> traversal = get_g_v1_out_jumpXx_t_out_hasNextX_in_jumpXyX_asXxX_out_asXyX_path(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final List<Path> paths = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(6, paths.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_jumpXxX_out_out_asXxX() {
        Traversal<Vertex, Vertex> traversal = get_g_V_jumpXxX_out_out_asXxX();
        printTraversalForm(traversal);
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


    public static class StandardTest extends JumpTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(final Object v1Id) {
            return g.v(v1Id).as("x").out().jump("x", h -> h.loops() < 2).value("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_loops_lt_2X() {
            return g.V().as("x").out().jump("x", t -> t.loops() < 2);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX() {
            return g.V().as("x").out().jump("x", t -> t.loops() < 2, t -> true);
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX_path() {
            return g.V().as("x").out().jump("x", t -> t.loops() < 2, t -> true).path();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_2_trueX_path() {
            return g.V().as("x").out().jump("x", 2, t -> true).path();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXxX_out_jumpXx_loops_lt_2X_asXyX_in_jumpXy_loops_lt_2X_name() {
            return g.V().as("x").out().jump("x", t -> t.loops() < 2).as("y").in().jump("y", t -> t.loops() < 2).value("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXxX_out_jumpXx_2X_asXyX_in_jumpXy_2X_name() {
            return g.V().as("x").out().jump("x", 2).as("y").in().jump("y", 2).value("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_2X() {
            return g.V().as("x").out().jump("x", 2);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_2_trueX() {
            return g.V().as("x").out().jump("x", 2, t -> true);
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_out_jumpXx_t_out_hasNextX_in_jumpXyX_asXxX_out_asXyX_path(final Object v1Id) {
            return g.v(v1Id).out().jump("x", t -> t.get().out().hasNext()).in().jump("y").as("x").out().as("y").path();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_jumpXxX_out_out_asXxX() {
            return g.V().jump("x").out().out().as("x");
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_asXaX_jumpXb_loops_gt_1X_out_jumpXaX_asXbX_name(final Object v1Id) {
            return g.v(v1Id).as("a").jump("b", t -> t.loops() > 1).out().jump("a").as("b").value("name");
        }
    }

    public static class ComputerTest extends JumpTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX(final Object v1Id) {
            return g.v(v1Id).as("x").out().jump("x", t -> t.loops() < 2).<String>value("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_loops_lt_2X() {
            return g.V().as("x").out().jump("x", t -> t.loops() < 2).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX() {
            return g.V().as("x").out().jump("x", t -> t.loops() < 2, t -> true).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2_trueX_path() {
            return g.V().as("x").out().jump("x", t -> t.loops() < 2, t -> true).path().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_2_trueX_path() {
            return g.V().as("x").out().jump("x", 2, t -> true).path().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXxX_out_jumpXx_loops_lt_2X_asXyX_in_jumpXy_loops_lt_2X_name() {
            return g.V().as("x").out().jump("x", t -> t.loops() < 2).as("y").in().jump("y", t -> t.loops() < 2).<String>value("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXxX_out_jumpXx_2X_asXyX_in_jumpXy_2X_name() {
            return g.V().as("x").out().jump("x", 2).as("y").in().jump("y", 2).<String>value("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_2X() {
            return g.V().as("x").out().jump("x", 2).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXxX_out_jumpXx_2_trueX() {
            return g.V().as("x").out().jump("x", 2, t -> true).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_out_jumpXx_t_out_hasNextX_in_jumpXyX_asXxX_out_asXyX_path(final Object v1Id) {
            return g.v(v1Id).out().jump("x", t -> t.get().out().hasNext()).in().jump("y").as("x").out().as("y").path().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_jumpXxX_out_out_asXxX() {
            return g.V().jump("x").out().out().as("x").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_asXaX_jumpXb_loops_gt_1X_out_jumpXaX_asXbX_name(final Object v1Id) {
            return g.v(v1Id).as("a").jump("b", t -> t.loops() > 1).out().jump("a").as("b").<String>value("name").submit(g.compute());
        }
    }
}
