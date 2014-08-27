package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class SelectTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_v1_asXaX_outXknowsX_asXbX_select(final Object v1Id);

    public abstract Traversal<Vertex, Map<String, String>> get_g_v1_asXaX_outXknowsX_asXbX_selectXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_v1_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id);

    public abstract Traversal<Vertex, Map<String, String>> get_g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_asXaX_outXknowsX_asXbX_select() {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_v1_asXaX_outXknowsX_asXbX_select(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Map<String, Vertex> bindings = traversal.next();
            assertEquals(2, bindings.size());
            assertEquals(convertToVertexId("marko"), (bindings.get("a")).id());
            assertTrue((bindings.get("b")).id().equals(convertToVertexId("vadas")) || bindings.get("b").id().equals(convertToVertexId("josh")));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_asXaX_outXknowsX_asXbX_selectXnameX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_v1_asXaX_outXknowsX_asXbX_selectXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Map<String, String> bindings = traversal.next();
            assertEquals(2, bindings.size());
            assertEquals("marko", bindings.get("a"));
            assertTrue(bindings.get("b").equals("josh") || bindings.get("b").equals("vadas"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_asXaX_outXknowsX_asXbX_selectXaX() {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_v1_asXaX_outXknowsX_asXbX_selectXaX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Map<String, Vertex> bindings = traversal.next();
            assertEquals(1, bindings.size());
            assertEquals(convertToVertexId("marko"), bindings.get("a").id());
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Map<String, String> bindings = traversal.next();
            assertEquals(1, bindings.size());
            assertEquals("marko", bindings.get("a"));
        }
        assertEquals(2, counter);
    }

    public static class JavaSelectTest extends SelectTest {
        public JavaSelectTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_v1_asXaX_outXknowsX_asXbX_select(final Object v1Id) {
            return g.v(v1Id).as("a").out("knows").as("b").select();
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_v1_asXaX_outXknowsX_asXbX_selectXnameX(final Object v1Id) {
            return g.v(v1Id).as("a").out("knows").as("b").select(v -> ((Vertex) v).value("name"));
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_v1_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id) {
            return g.v(v1Id).as("a").out("knows").as("b").select(Arrays.asList("a"));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX(final Object v1Id) {
            return g.v(v1Id).as("a").out("knows").as("b").select(Arrays.asList("a"), v -> ((Vertex) v).value("name"));
        }
    }

    public static class JavaComputerSelectTest extends SelectTest {
        public JavaComputerSelectTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_v1_asXaX_outXknowsX_asXbX_select(final Object v1Id) {
            return g.v(v1Id).as("a").out("knows").as("b").<Vertex>select().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_v1_asXaX_outXknowsX_asXbX_selectXnameX(final Object v1Id) {
            // TODO: Micro elements do not store properties
            return g.v(v1Id).as("a").out("knows").as("b").select(v -> ((Vertex) v).value("name")); //.submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_v1_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id) {
            return g.v(v1Id).as("a").out("knows").as("b").<Vertex>select(Arrays.asList("a")).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX(final Object v1Id) {
            // TODO: Micro elements do not store properties
            return g.v(v1Id).as("a").out("knows").as("b").select(Arrays.asList("a"), v -> ((Vertex) v).value("name"));  // .submit(g.compute());
        }
    }
}
