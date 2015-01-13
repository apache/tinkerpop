package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class SelectTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_VX1X_asXaX_outXknowsX_asXbX_select(final Object v1Id);

    public abstract Traversal<Vertex, Map<String, String>> get_g_VX1X_asXaX_outXknowsX_asXbX_select_byXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_select_byXnameX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_aggregate_asXbX_select_byXnameX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_asXaX_name_order_asXbX_select_byXnameX_by();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXaX_outXknowsX_asXbX_select() {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_VX1X_asXaX_outXknowsX_asXbX_select(convertToVertexId("marko"));
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
    public void g_VX1X_asXaX_outXknowsX_asXbX_select_byXnameX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_VX1X_asXaX_outXknowsX_asXbX_select_byXnameX(convertToVertexId("marko"));
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
    public void g_VX1X_asXaX_outXknowsX_asXbX_selectXaX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            assertEquals(convertToVertexId("marko"), vertex.id());
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals("marko", traversal.next());
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_out_asXbX_select_byXnameX() {
        Arrays.asList(
                get_g_V_asXaX_out_asXbX_select_byXnameX(),
                get_g_V_asXaX_out_aggregate_asXbX_select_byXnameX()).forEach(traversal -> {
            printTraversalForm(traversal);
            final List<Map<String, String>> expected = makeMapList(2,
                    "a", "marko", "b", "lop",
                    "a", "marko", "b", "vadas",
                    "a", "marko", "b", "josh",
                    "a", "josh", "b", "ripple",
                    "a", "josh", "b", "lop",
                    "a", "peter", "b", "lop");
            checkResults(expected, traversal);
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_name_order_asXbX_select_byXnameX_byXitX() {
        Arrays.asList(
                get_g_V_asXaX_name_order_asXbX_select_byXnameX_by()).forEach(traversal -> {
            printTraversalForm(traversal);
            final List<Map<String, String>> expected = makeMapList(2,
                    "a", "marko", "b", "marko",
                    "a", "vadas", "b", "vadas",
                    "a", "josh", "b", "josh",
                    "a", "ripple", "b", "ripple",
                    "a", "lop", "b", "lop",
                    "a", "peter", "b", "peter");
            checkResults(expected, traversal);
        });
    }


    public static class StandardTest extends SelectTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_VX1X_asXaX_outXknowsX_asXbX_select(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").select();
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_VX1X_asXaX_outXknowsX_asXbX_select_byXnameX(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").<String>select().by("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").select("a");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").<String>select("a").by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_select_byXnameX() {
            return g.V().as("a").out().as("b").<String>select().by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_aggregate_asXbX_select_byXnameX() {
            return g.V().as("a").out().aggregate().as("b").<String>select().by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_name_order_asXbX_select_byXnameX_by() {
            return g.V().as("a").values("name").order().as("b").<String>select().by("name").by();
        }


    }

    public static class ComputerTest extends SelectTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_VX1X_asXaX_outXknowsX_asXbX_select(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").<Vertex>select().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_VX1X_asXaX_outXknowsX_asXbX_select_byXnameX(final Object v1Id) {
            // TODO: Micro elements do not store properties
            return g.V(v1Id).as("a").out("knows").as("b").<String>select().by("name"); //.submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id) {
            return g.V(v1Id).as("a").out("knows").as("b").<Vertex>select("a").submit(g.compute());  // TODO
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX(final Object v1Id) {
            // TODO: Micro elements do not store properties
            return g.V(v1Id).as("a").out("knows").as("b").<String>select("a").by("name");  // .submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_select_byXnameX() {
            // TODO: Micro elements do not store properties
            return g.V().as("a").out().as("b").<String>select().by("name");  // .submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_aggregate_asXbX_select_byXnameX() {
            // TODO: Micro elements do not store properties
            return g.V().as("a").out().aggregate().as("b").<String>select().by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_name_order_asXbX_select_byXnameX_by() {
            // TODO: Micro elements do not store properties
            return g.V().as("a").values("name").order().as("b").<String>select().by("name").by();
        }
    }
}
