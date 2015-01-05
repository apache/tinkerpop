package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class RepeatTest extends AbstractGremlinProcessTest {

    // DO/WHILE

    public abstract Traversal<Vertex, String> get_g_VX1X_repeatXoutX_untilXloops_gte_2X_name(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X();

    public abstract Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X_emit();

    public abstract Traversal<Vertex, Path> get_g_V_repeatXoutX_untilXloops_gte_2X_emit_path();

    public abstract Traversal<Vertex, Path> get_g_V_repeatXoutX_untilX2X_emit_path();

    public abstract Traversal<Vertex, String> get_g_V_repeatXoutX_untilXloops_gte_2X_repeatXinX_untilXloops_gte_2X_name();

    public abstract Traversal<Vertex, String> get_g_V_repeatXoutX_untilX2X_repeatXinX_untilX2X_name();

    public abstract Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilX2X();

    public abstract Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilX2X_emit();

    // WHILE/DO

    /*
    public abstract Traversal<Vertex, String> get_g_v1_untilXa_loops_gt_1X_out_asXaX_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_v1_untilXa_1X_out_asXaX_name(final Object v1Id);
     */

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_asXxX_out_jumpXx_loops_lt_2X_valueXnameX() {
        final List<Traversal<Vertex, String>> traversals = new ArrayList<>();
        traversals.add(get_g_VX1X_repeatXoutX_untilXloops_gte_2X_name(convertToVertexId("marko")));
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
        traversals.add(get_g_V_repeatXoutX_untilXloops_gte_2X_emit_path());
        traversals.add(get_g_V_repeatXoutX_untilX2X_emit_path());
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
        traversals.add(get_g_V_repeatXoutX_untilXloops_gte_2X_repeatXinX_untilXloops_gte_2X_name());
        traversals.add(get_g_V_repeatXoutX_untilX2X_repeatXinX_untilX2X_name());
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
        traversals.add(get_g_V_repeatXoutX_untilX2X());
        traversals.add(get_g_V_repeatXoutX_untilXloops_gte_2X());
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
        traversals.add(get_g_V_repeatXoutX_untilX2X_emit());
        traversals.add(get_g_V_repeatXoutX_untilXloops_gte_2X_emit());
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

    public static class StandardTest extends RepeatTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_repeatXoutX_untilXloops_gte_2X_name(final Object v1Id) {
            return g.V(v1Id).repeat(g.<Vertex>of().out()).until(t -> t.loops() >= 2).values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X() {
            return g.V().repeat(g.<Vertex>of().out()).until(t -> t.loops() >= 2);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X_emit() {
            return g.V().repeat(g.<Vertex>of().out()).until(t -> t.loops() >= 2).emit();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_untilXloops_gte_2X_emit_path() {
            return g.V().repeat(g.<Vertex>of().out()).until(t -> t.loops() >= 2).emit().path();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_untilX2X_emit_path() {
            return g.V().repeat(g.<Vertex>of().out()).until(2).emit().path();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_untilXloops_gte_2X_repeatXinX_untilXloops_gte_2X_name() {
            return g.V().repeat(g.<Vertex>of().out()).until(t -> t.loops() >= 2).repeat(g.<Vertex>of().in()).until(t -> t.loops() >= 2).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_untilX2X_repeatXinX_untilX2X_name() {
            return g.V().repeat(g.<Vertex>of().out()).until(2).repeat(g.<Vertex>of().in()).until(2).values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilX2X() {
            return g.V().repeat(g.<Vertex>of().out()).until(2);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilX2X_emit() {
            return g.V().repeat(g.<Vertex>of().out()).until(2).emit();
        }
    }

    public static class ComputerTest extends RepeatTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_repeatXoutX_untilXloops_gte_2X_name(final Object v1Id) {
            return g.V(v1Id).repeat(g.<Vertex>of().out()).until(t -> t.loops() >= 2).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X() {
            return g.V().repeat(g.<Vertex>of().out()).until(t -> t.loops() >= 2).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X_emit() {
            return g.V().repeat(g.<Vertex>of().out()).until(t -> t.loops() >= 2).emit().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_untilXloops_gte_2X_emit_path() {
            return g.V().repeat(g.<Vertex>of().out()).until(t -> t.loops() >= 2).emit().path().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_untilX2X_emit_path() {
            return g.V().repeat(g.<Vertex>of().out()).until(2).emit().path().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_untilXloops_gte_2X_repeatXinX_untilXloops_gte_2X_name() {
            return g.V().repeat(g.<Vertex>of().out()).until(t -> t.loops() >= 2).repeat(g.<Vertex>of().in()).until(t -> t.loops() >= 2).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_untilX2X_repeatXinX_untilX2X_name() {
            return g.V().repeat(g.<Vertex>of().out()).until(2).repeat(g.<Vertex>of().in()).until(2).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilX2X() {
            return g.V().repeat(g.<Vertex>of().out()).until(2).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilX2X_emit() {
            return g.V().repeat(g.<Vertex>of().out()).until(2).emit().submit(g.compute());
        }
    }
}