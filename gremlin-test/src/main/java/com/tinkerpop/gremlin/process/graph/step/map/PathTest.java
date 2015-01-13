package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class PathTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Path> get_g_VX1X_name_path(final Object v1Id);

    public abstract Traversal<Vertex, Path> get_g_VX1X_out_path_byXageX_byXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Path> get_g_V_repeatXoutX_timesX2X_path_by_byXnameX_byXlangX();

    public abstract Traversal<Vertex, Path> get_g_V_out_out_path_byXnameX_byXageX();

    public abstract Traversal<Vertex, Path> get_g_V_asXaX_hasXname_markoX_asXbX_hasXage_29X_asXcX_path();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_name_path() {
        final Traversal<Vertex, Path> traversal = get_g_VX1X_name_path(convertToVertexId("marko"));
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
    public void g_VX1X_out_path_byXageX_byXnameX() {
        final Traversal<Vertex, Path> traversal = get_g_VX1X_out_path_byXageX_byXnameX(convertToVertexId("marko"));
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
    public void g_V_repeatXoutX_timesX2X_path_byXitX_byXnameX_byXlangX() {
        final Traversal<Vertex, Path> traversal = get_g_V_repeatXoutX_timesX2X_path_by_byXnameX_byXlangX();
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
    public void g_V_out_out_path_byXnameX_byXageX() {
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

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_hasXname_markoX_asXbX_hasXage_29X_asXcX_path() {
        final Traversal<Vertex, Path> traversal = get_g_V_asXaX_hasXname_markoX_asXbX_hasXage_29X_asXcX_path();
        printTraversalForm(traversal);
        final Path path = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(1, path.size());
        assertTrue(path.hasLabel("a"));
        assertTrue(path.hasLabel("b"));
        assertTrue(path.hasLabel("c"));
        assertEquals(1, path.labels().size());
        assertEquals(3, path.labels().get(0).size());
    }

    public static class StandardTest extends PathTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_name_path(final Object v1Id) {
            return g.V(v1Id).values("name").path();
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_out_path_byXageX_byXnameX(final Object v1Id) {
            return g.V(v1Id).out().path().by("age").by("name");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_timesX2X_path_by_byXnameX_byXlangX() {
            return g.V().repeat(__.out()).times(2).path().by().by("name").by("lang");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_out_path_byXnameX_byXageX() {
            return g.V().out().out().path().by("name").by("age");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXaX_hasXname_markoX_asXbX_hasXage_29X_asXcX_path() {
            return g.V().as("a").has("name", "marko").as("b").has("age", 29).as("c").path();
        }
    }

    public static class ComputerTest extends PathTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_name_path(final Object v1Id) {
            return g.V(v1Id).identity().values("name").path().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_out_path_byXageX_byXnameX(final Object v1Id) {
            // TODO: Detached elements do not store properties (attach)
            return g.V(v1Id).out().path().by("age").by("name"); // .submit(g.compute())
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_timesX2X_path_by_byXnameX_byXlangX() {
            // TODO: Detached elements do not store properties (attach)
            return g.V().repeat(__.out()).times(2).path().by().by("name").by("lang");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_out_path_byXnameX_byXageX() {
            // TODO: Detached elements do not store properties (attach)
            return g.V().out().out().path().by("name").by("age");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXaX_hasXname_markoX_asXbX_hasXage_29X_asXcX_path() {
            return g.V().as("a").has("name", "marko").as("b").has("age", 29).as("c").path().submit(g.compute());
        }
    }
}