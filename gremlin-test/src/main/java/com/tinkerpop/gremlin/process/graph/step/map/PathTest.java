package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class PathTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Path> get_g_v1_name_path(final Object v1Id);

    public abstract Traversal<Vertex, Path> get_g_v1_out_path_byXageX_byXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2X_path_byXitX_byXnameX_byXlangX();

    public abstract Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_2X_path_byXitX_byXnameX_byXlangX();

    public abstract Traversal<Vertex, Path> get_g_V_out_out_path_byXnameX_byXageX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_name_path() {
        final Traversal<Vertex, Path> traversal = get_g_v1_name_path(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final Path path = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, path.size());
        assertEquals(convertToVertexId("marko"), ((Vertex) path.get(0)).<String>id());
        assertEquals("marko", path.<String>get(1));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_out_pathXage_nameX() {
        final Traversal<Vertex, Path> traversal = get_g_v1_out_path_byXageX_byXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        final Set<String> names = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Path path = traversal.next();
            assertEquals(Integer.valueOf(29), path.<Integer>get(0));
            assertTrue(path.get(1).equals("josh") || path.get(1).equals("vadas") || path.get(1).equals("lop"));
            names.add(path.get(1));
        }
        assertEquals(3, counter);
        assertEquals(3, names.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXxX_out_loopXx_loops_lt_2X_pathXit__name__langX() {
        final Traversal<Vertex, Path> traversal = get_g_V_asXxX_out_jumpXx_loops_lt_2X_path_byXitX_byXnameX_byXlangX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Path path = traversal.next();
            assertEquals(3, path.size());
            assertEquals("marko", ((Vertex) path.get(0)).<String>value("name"));
            assertEquals("josh", path.<String>get(1));
            assertEquals("java", path.<String>get(2));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXxX_out_loopXx_2X_pathXit_name_langX() {
        final Traversal<Vertex, Path> traversal = get_g_V_asXxX_out_jumpXx_2X_path_byXitX_byXnameX_byXlangX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Path path = traversal.next();
            assertEquals(3, path.size());
            assertEquals("marko", ((Vertex) path.get(0)).<String>value("name"));
            assertEquals("josh", path.<String>get(1));
            assertEquals("java", path.<String>get(2));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_pathXname_ageX() {
        final Traversal<Vertex, Path> traversal = get_g_V_out_out_path_byXnameX_byXageX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Path path = traversal.next();
            assertEquals(3, path.size());
            assertEquals("marko", path.<String>get(0));
            assertEquals(Integer.valueOf(32), path.<Integer>get(1));
            assertTrue(path.get(2).equals("lop") || path.get(2).equals("ripple"));
        }
        assertEquals(2, counter);
    }

    public static class StandardTest extends PathTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_name_path(final Object v1Id) {
            return g.V(v1Id).values("name").path();
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_out_path_byXageX_byXnameX(final Object v1Id) {
            return g.V(v1Id).out().path().by("age").by("name");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2X_path_byXitX_byXnameX_byXlangX() {
            return g.V().as("x").out().jump("x", o -> o.loops() < 2).path().by(Function.identity()).by("name").by("lang");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_2X_path_byXitX_byXnameX_byXlangX() {
            return g.V().as("x").out().jump("x", 2).path().by(Function.identity()).by("name").by("lang");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_out_path_byXnameX_byXageX() {
            return g.V().out().out().path().by("name").by("age");
        }
    }

    public static class ComputerTest extends PathTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_name_path(final Object v1Id) {
            return g.V(v1Id).identity().values("name").path().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_out_path_byXageX_byXnameX(final Object v1Id) {
            // TODO: Detached elements do not store properties (attach)
            return g.V(v1Id).out().path().by("age").by("name"); // .submit(g.compute())
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2X_path_byXitX_byXnameX_byXlangX() {
            // TODO: Detached elements do not store properties (attach)
            return g.V().as("x").out().jump("x", t -> t.loops() < 2).path().by(Function.identity()).by("name").by("lang");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_2X_path_byXitX_byXnameX_byXlangX() {
            // TODO: Detached elements do not store properties (attach)
            return g.V().as("x").out().jump("x", 2).path().by(Function.identity()).by("name").by("lang");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_out_path_byXnameX_byXageX() {
            // TODO: Detached elements do not store properties (attach)
            return g.V().out().out().path().by("name").by("age");
        }
    }
}