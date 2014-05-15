package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.util.As;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Iterator;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class SelectTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex,Path> get_g_v1_asXaX_outXknowsX_asXbX_select(final Object v1Id);

    public abstract Traversal<Vertex,Path> get_g_v1_asXaX_outXknowsX_asXbX_selectXnameX(final Object v1Id);

    public abstract Traversal<Vertex,Path> get_g_v1_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id);

    public abstract Traversal<Vertex,Path> get_g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX(final Object v1Id);

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_asXaX_outXknowsX_asXbX_select() {
        final Iterator<Path> step = get_g_v1_asXaX_outXknowsX_asXbX_select(convertToVertexId("marko"));
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            Path path = step.next();
            assertEquals(2, path.size());
            assertEquals(convertToVertexId("marko"), ((Vertex) path.get(0)).getId());
            assertTrue(((Vertex) path.get(1)).getId().equals(convertToVertexId("vadas")) || ((Vertex) path.get(1)).getId().equals(convertToVertexId("josh")));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_asXaX_outXknowsX_asXbX_selectXnameX() {
        final Iterator<Path> step = get_g_v1_asXaX_outXknowsX_asXbX_selectXnameX(convertToVertexId("marko"));
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            Path path = step.next();
            assertEquals(2, path.size());
            assertEquals("marko", path.get(0).toString());
            assertTrue(path.get(1).equals("josh") || path.get(1).equals("vadas"));
            assertTrue(path.get("b").equals("josh") || path.get("b").equals("vadas"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_asXaX_outXknowsX_asXbX_selectXaX() {
        final Iterator<Path> step = get_g_v1_asXaX_outXknowsX_asXbX_selectXaX(convertToVertexId("marko"));
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            Path path = step.next();
            assertEquals(1, path.size());
            assertEquals(convertToVertexId("marko"), ((Vertex) path.get(0)).getId());
            assertEquals(convertToVertexId("marko"), ((Vertex) path.get("a")).getId());
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX() {
        final Iterator<Path> step = get_g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX(convertToVertexId("marko"));
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            Path path = step.next();
            assertEquals(1, path.size());
            assertEquals("marko", path.get(0).toString());
            assertEquals("marko", path.get("a").toString());
        }
        assertEquals(2, counter);
    }

    public static class JavaSelectTest extends SelectTest {
        public JavaSelectTest() {
            requiresGraphComputer = false;
        }

        public Traversal<Vertex,Path> get_g_v1_asXaX_outXknowsX_asXbX_select(final Object v1Id) {
            return g.v(v1Id).as("a").out("knows").as("b").select();
        }

        public Traversal<Vertex,Path> get_g_v1_asXaX_outXknowsX_asXbX_selectXnameX(final Object v1Id) {
            return g.v(v1Id).as("a").out("knows").as("b").select(v -> ((Vertex) v).getValue("name"));
        }

        public Traversal<Vertex,Path> get_g_v1_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id) {
            return g.v(v1Id).as("a").out("knows").as("b").select(As.of("a"));
        }

        public Traversal<Vertex,Path> get_g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX(final Object v1Id) {
            return g.v(v1Id).as("a").out("knows").as("b").select(As.of("a"), v -> ((Vertex) v).getValue("name"));
        }
    }

    public static class JavaComputerSelectTest extends SelectTest {
        public JavaComputerSelectTest() {
            requiresGraphComputer = true;
        }

        public Traversal<Vertex,Path> get_g_v1_asXaX_outXknowsX_asXbX_select(final Object v1Id) {
            return g.v(v1Id).as("a").out("knows").as("b").select().submit(g.compute());
        }

        public Traversal<Vertex,Path> get_g_v1_asXaX_outXknowsX_asXbX_selectXnameX(final Object v1Id) {
            // TODO: Micro elements do not store properties
            return g.v(v1Id).as("a").out("knows").as("b").select(v -> ((Vertex) v).getValue("name")); //.submit(g.compute());
        }

        public Traversal<Vertex,Path> get_g_v1_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id) {
            return g.v(v1Id).as("a").out("knows").as("b").select(As.of("a")).submit(g.compute());
        }

        public Traversal<Vertex,Path> get_g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX(final Object v1Id) {
            // TODO: Micro elements do not store properties
            return g.v(v1Id).as("a").out("knows").as("b").select(As.of("a"), v -> ((Vertex) v).getValue("name"));  // .submit(g.compute());
        }
    }
}
