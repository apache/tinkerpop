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
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class PathTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Path> get_g_v1_valueXnameX_path(final Object v1Id);

    public abstract Traversal<Vertex, Path> get_g_v1_out_pathXage_nameX(final Object v1Id);

    public abstract Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2X_pathXit__name__langX();

    public abstract Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_2X_pathXit_name_langX();

    public abstract Traversal<Vertex, Path> get_g_V_out_out_pathXname_ageX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_valueXnameX_path() {
        final Traversal<Vertex, Path> traversal = get_g_v1_valueXnameX_path(convertToVertexId("marko"));
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
        final Traversal<Vertex, Path> traversal = get_g_v1_out_pathXage_nameX(convertToVertexId("marko"));
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
        final Traversal<Vertex, Path> traversal = get_g_V_asXxX_out_jumpXx_loops_lt_2X_pathXit__name__langX();
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
        final Traversal<Vertex, Path> traversal = get_g_V_asXxX_out_jumpXx_2X_pathXit_name_langX();
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
        final Traversal<Vertex, Path> traversal = get_g_V_out_out_pathXname_ageX();
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
        public Traversal<Vertex, Path> get_g_v1_valueXnameX_path(final Object v1Id) {
            return g.v(v1Id).identity().value("name").path();
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_out_pathXage_nameX(final Object v1Id) {
            return g.v(v1Id).out().path(v -> ((Vertex) v).value("age"), v -> ((Vertex) v).value("name"));
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2X_pathXit__name__langX() {
            return g.V().as("x").out()
                    .jump("x", o -> o.loops() < 2)
                    .path(v -> v, v -> ((Vertex) v).value("name"), v -> ((Vertex) v).value("lang"));
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_2X_pathXit_name_langX() {
            return g.V().as("x").out()
                    .jump("x", 2)
                    .path(v -> v, v -> ((Vertex) v).value("name"), v -> ((Vertex) v).value("lang"));
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_out_pathXname_ageX() {
            return g.V().out().out().path(v -> ((Vertex) v).value("name"), v -> ((Vertex) v).value("age"));
        }
    }

    public static class ComputerTest extends PathTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_valueXnameX_path(final Object v1Id) {
            return g.v(v1Id).identity().value("name").path().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_out_pathXage_nameX(final Object v1Id) {
            // TODO: Detached elements do not store properties (attach)
            return g.v(v1Id).out().path(v -> ((Vertex) v).value("age"), v -> ((Vertex) v).value("name")); // .submit(g.compute())
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2X_pathXit__name__langX() {
            // TODO: Detached elements do not store properties (attach)
            return g.V().as("x").out()
                    .jump("x", t -> t.loops() < 2)
                    .path(v -> v, v -> ((Vertex) v).value("name"), v -> ((Vertex) v).value("lang")); // .submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_2X_pathXit_name_langX() {
            // TODO: Detached elements do not store properties (attach)
            return g.V().as("x").out()
                    .jump("x", 2)
                    .path(v -> v, v -> ((Vertex) v).value("name"), v -> ((Vertex) v).value("lang"));
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_out_pathXname_ageX() {
            // TODO: Detached elements do not store properties (attach)
            return g.V().out().out().path(v -> ((Vertex) v).value("name"), v -> ((Vertex) v).value("age"));
        }
    }
}