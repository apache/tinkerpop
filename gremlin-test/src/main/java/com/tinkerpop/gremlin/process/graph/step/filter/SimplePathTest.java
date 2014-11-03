package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class SimplePathTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_simplePath(final Object v1Id);

    public abstract Traversal<Vertex, Path> get_g_V_asXxX_both_simplePath_jumpXx_loops_lt_3X_path();

    public abstract Traversal<Vertex, Path> get_g_V_asXxX_both_simplePath_jumpXx_3X_path();

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outXcreatedX_inXcreatedX_simplePath() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_outXcreatedX_inXcreatedX_simplePath(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            assertTrue(vertex.value("name").equals("josh") || vertex.value("name").equals("peter"));
        }
        assertEquals(2, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXxX_both_simplePath_jumpXx_loops_lt_3X_path() {
        final Traversal<Vertex, Path> traversal = get_g_V_asXxX_both_simplePath_jumpXx_loops_lt_3X_path();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertTrue(traversal.next().isSimple());
        }
        assertEquals(18, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXxX_both_simplePath_jumpXx_3X_path() {
        final Traversal<Vertex, Path> traversal = get_g_V_asXxX_both_simplePath_jumpXx_3X_path();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertTrue(traversal.next().isSimple());
        }
        assertEquals(18, counter);
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends SimplePathTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_simplePath(final Object v1Id) {
            return g.v(v1Id).out("created").in("created").simplePath();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_both_simplePath_jumpXx_loops_lt_3X_path() {
            return g.V().as("x").both().simplePath().jump("x", t -> t.loops() < 3).path();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_both_simplePath_jumpXx_3X_path() {
            return g.V().as("x").both().simplePath().jump("x", 3).path();
        }
    }

    public static class ComputerTest extends SimplePathTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_simplePath(final Object v1Id) {
            return g.v(v1Id).out("created").in("created").simplePath().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_both_simplePath_jumpXx_loops_lt_3X_path() {
            return g.V().as("x").both().simplePath().jump("x", t -> t.loops() < 3).path().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_both_simplePath_jumpXx_3X_path() {
            return g.V().as("x").both().simplePath().jump("x", 3).path().submit(g.compute());
        }
    }
}
