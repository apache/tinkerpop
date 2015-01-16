package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__;
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

    public abstract Traversal<Vertex, Path> get_g_V_repeatXoutX_timesX2X_emit_path();

    public abstract Traversal<Vertex, String> get_g_V_repeatXoutX_untilXloops_gte_2X_repeatXinX_untilXloops_gte_2X_name();

    public abstract Traversal<Vertex, String> get_g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name();

    public abstract Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X();

    public abstract Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X_emit();

    // WHILE/DO

    public abstract Traversal<Vertex, String> get_g_VX1X_untilXloops_gte_2X_repeatXoutX_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_timesX2X_repeatXoutX_name(final Object v1Id);

    public abstract Traversal<Vertex, Path> get_g_V_emit_timesX2X_repeatXoutX_path();

    public abstract Traversal<Vertex, Path> get_g_V_emit_repeatXoutX_timesX2X_path();

    public abstract Traversal<Vertex, String> get_g_V_emitXhasXlabel_personXX_repeatXoutX_name(final Object v1Id);

    // SIDE-EFFECTS

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_repeatXoutX_untilXloops_gte_2X_name() {
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
    public void g_V_repeatXoutX_timesX2X_emit_path() {
        final List<Traversal<Vertex, Path>> traversals = new ArrayList<>();
        traversals.add(get_g_V_repeatXoutX_untilXloops_gte_2X_emit_path());
        traversals.add(get_g_V_repeatXoutX_timesX2X_emit_path());
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
    public void g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name() {
        final List<Traversal<Vertex, String>> traversals = new ArrayList<>();
        traversals.add(get_g_V_repeatXoutX_untilXloops_gte_2X_repeatXinX_untilXloops_gte_2X_name());
        traversals.add(get_g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            checkResults(Arrays.asList("marko","marko"),traversal);
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXoutX_timesX2X() {
        final List<Traversal<Vertex, Vertex>> traversals = new ArrayList<>();
        traversals.add(get_g_V_repeatXoutX_timesX2X());
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
    public void g_V_repeatXoutX_timesX2X_emit() {
        final List<Traversal<Vertex, Vertex>> traversals = new ArrayList<>();
        traversals.add(get_g_V_repeatXoutX_timesX2X_emit());
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

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_timesX2X_repeatXoutX_name() {
        final List<Traversal<Vertex, String>> traversals = Arrays.asList(
                get_g_VX1X_untilXloops_gte_2X_repeatXoutX_name(convertToVertexId("marko")),
                get_g_VX1X_timesX2X_repeatXoutX_name(convertToVertexId("marko")));
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            checkResults(Arrays.asList("lop", "ripple"), traversal);
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_emit_timesX2X_repeatXoutX_path() {
        final List<Traversal<Vertex, Path>> traversals = Arrays.asList(
                get_g_V_emit_timesX2X_repeatXoutX_path(),
                get_g_V_emit_repeatXoutX_timesX2X_path());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            int path1 = 0;
            int path2 = 0;
            int path3 = 0;
            while (traversal.hasNext()) {
                final Path path = traversal.next();
                if (path.size() == 1) {
                    path1++;
                } else if (path.size() == 2) {
                    path2++;
                } else if (path.size() == 3) {
                    path3++;
                } else {
                    fail("Only path lengths of 1, 2, or 3 should be seen");
                }
            }
            assertEquals(6, path1);
            assertEquals(6, path2);
            assertEquals(2, path3);
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_emitXhasXlabel_personXX_repeatXoutX_name() {
        final List<Traversal<Vertex, String>> traversals = Arrays.asList(get_g_V_emitXhasXlabel_personXX_repeatXoutX_name(convertToVertexId("marko")));
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            checkResults(Arrays.asList("marko", "josh", "vadas"), traversal);
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX() {
        final List<Traversal<Vertex, Map<String, Long>>> traversals = Arrays.asList(get_g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Map<String, Long> map = traversal.next();
            assertFalse(traversal.hasNext());
            //[ripple:2, peter:1, vadas:2, josh:2, lop:4, marko:1]
            assertEquals(6, map.size());
            assertEquals(1l, map.get("marko").longValue());
            assertEquals(2l, map.get("vadas").longValue());
            assertEquals(2l, map.get("josh").longValue());
            assertEquals(4l, map.get("lop").longValue());
            assertEquals(2l, map.get("ripple").longValue());
            assertEquals(1l, map.get("peter").longValue());
        });
    }

    public static class StandardTest extends RepeatTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_repeatXoutX_untilXloops_gte_2X_name(final Object v1Id) {
            return g.V(v1Id).repeat(__.out()).until(t -> t.loops() >= 2).values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X() {
            return g.V().repeat(__.out()).until(t -> t.loops() >= 2);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X_emit() {
            return g.V().repeat(__.out()).until(t -> t.loops() >= 2).emit();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_untilXloops_gte_2X_emit_path() {
            return g.V().repeat(__.out()).until(t -> t.loops() >= 2).emit().path();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_timesX2X_emit_path() {
            return g.V().repeat(__.out()).times(2).emit().path();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_untilXloops_gte_2X_repeatXinX_untilXloops_gte_2X_name() {
            return g.V().repeat(__.out()).until(t -> t.loops() >= 2).repeat(__.in()).until(t -> t.loops() >= 2).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name() {
            return g.V().repeat(__.out()).times(2).repeat(__.in()).times(2).values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X() {
            return g.V().repeat(__.out()).times(2);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X_emit() {
            return g.V().repeat(__.out()).times(2).emit();
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_untilXloops_gte_2X_repeatXoutX_name(Object v1Id) {
            return g.V(v1Id).until(t -> t.loops() >= 2).repeat(__.out()).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_timesX2X_repeatXoutX_name(Object v1Id) {
            return g.V(v1Id).times(2).repeat(__.out()).values("name");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_emit_repeatXoutX_timesX2X_path() {
            return g.V().emit().repeat(__.out()).times(2).path();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_emit_timesX2X_repeatXoutX_path() {
            return g.V().emit().times(2).repeat(__.out()).path();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_emitXhasXlabel_personXX_repeatXoutX_name(final Object v1Id) {
            return g.V(v1Id).emit(__.has(T.label, "person")).repeat(__.out()).values("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX() {
            return g.V().repeat(__.groupCount("m").by("name").out()).times(2).cap("m");
        }
    }

    public static class ComputerTest extends RepeatTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_repeatXoutX_untilXloops_gte_2X_name(final Object v1Id) {
            return g.V(v1Id).repeat(__.out()).until(t -> t.loops() >= 2).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X() {
            return g.V().repeat(__.out()).until(t -> t.loops() >= 2).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_untilXloops_gte_2X_emit() {
            return g.V().repeat(__.out()).until(t -> t.loops() >= 2).emit().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_untilXloops_gte_2X_emit_path() {
            return g.V().repeat(__.out()).until(t -> t.loops() >= 2).emit().path().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_timesX2X_emit_path() {
            return g.V().repeat(__.out()).times(2).emit().path().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_untilXloops_gte_2X_repeatXinX_untilXloops_gte_2X_name() {
            return g.V().repeat(__.out()).until(t -> t.loops() >= 2).repeat(__.in()).until(t -> t.loops() >= 2).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name() {
            return g.V().repeat(__.out()).times(2).repeat(__.in()).times(2).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X() {
            return g.V().repeat(__.out()).times(2).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X_emit() {
            return g.V().repeat(__.out()).times(2).emit().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_untilXloops_gte_2X_repeatXoutX_name(Object v1Id) {
            return g.V(v1Id).until(t -> t.loops() >= 2).repeat(__.out()).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_timesX2X_repeatXoutX_name(Object v1Id) {
            return g.V(v1Id).times(2).repeat(__.out()).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_emit_repeatXoutX_timesX2X_path() {
            return g.V().emit().repeat(__.out()).times(2).path().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_emit_timesX2X_repeatXoutX_path() {
            return g.V().emit().times(2).repeat(__.out()).path().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_emitXhasXlabel_personXX_repeatXoutX_name(final Object v1Id) {
            return g.V(v1Id).emit(__.has(T.label, "person")).repeat(__.out()).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX() {
            return g.V().repeat(__.groupCount("m").by("name").out()).times(2).<Map<String, Long>>cap("m").submit(g.compute());
        }
    }
}