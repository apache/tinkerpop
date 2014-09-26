package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class BackTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_v1_asXhereX_out_backXhereX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX(final Object v4Id);

    public abstract Traversal<Vertex, String> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX(final Object v4Id);

    public abstract Traversal<Vertex, Edge> get_g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX(final Object v1Id);

    public abstract Traversal<Vertex, Edge> get_g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX(final Object v1Id);

    public abstract Traversal<Vertex, Edge> get_g_v1_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_backXhereX(final Object v1Id);

    public abstract Traversal<Vertex, Edge> get_g_v1_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_backXhereX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_asXhereXout_valueXnameX_backXhereX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_asXhereX_out_backXhereX() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_asXhereX_out_backXhereX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals("marko", traversal.next().<String>value("name"));
        }
        assertEquals(3, counter);
    }


    @Test
    @LoadGraphWith(MODERN)
    public void g_v4_out_asXhereX_hasXlang_javaX_backXhereX() {
        final Traversal<Vertex, Vertex> traversal = get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            assertEquals("java", vertex.<String>value("lang"));
            assertTrue(vertex.value("name").equals("ripple") || vertex.value("name").equals("lop"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX() {
        final Traversal<Vertex, Edge> traversal = get_g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final Edge edge = traversal.next();
        assertEquals("knows", edge.label());
        assertEquals(convertToVertexId("vadas"), edge.inV().id().next());
        assertEquals(convertToVertexId("marko"), edge.outV().id().next());
        assertEquals(0.5d, edge.<Double>value("weight"), 0.0001d);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX() {
        final Traversal<Vertex, String> traversal = get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        final Set<String> names = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            names.add(traversal.next());
        }
        assertEquals(2, counter);
        assertEquals(2, names.size());
        assertTrue(names.contains("ripple"));
        assertTrue(names.contains("lop"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX() {
        final List<Traversal<Vertex, Edge>> traversals = Arrays.asList(
                get_g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX(convertToVertexId("marko")),
                get_g_v1_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_backXhereX(convertToVertexId("marko")),
                get_g_v1_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_backXhereX(convertToVertexId("marko")));
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            assertTrue(traversal.hasNext());
            assertTrue(traversal.hasNext());
            final Edge edge = traversal.next();
            assertEquals("knows", edge.label());
            assertEquals(1.0d, edge.<Double>value("weight"), 0.00001d);
            assertFalse(traversal.hasNext());
            assertFalse(traversal.hasNext());
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXhereXout_valueXnameX_backXhereX() {
        Traversal<Vertex, Vertex> traversal = get_g_V_asXhereXout_valueXnameX_backXhereX();
        super.checkResults(new HashMap<Vertex, Long>() {{
            put(convertToVertex(g, "marko"), 3l);
            put(convertToVertex(g, "josh"), 2l);
            put(convertToVertex(g, "peter"), 1l);
        }}, traversal);
    }

    public static class StandardTest extends BackTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_asXhereX_out_backXhereX(final Object v1Id) {
            return g.v(v1Id).as("here").out().back("here");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX(final Object v4Id) {
            return g.v(v4Id).out().as("here").has("lang", "java").back("here");
        }

        @Override
        public Traversal<Vertex, String> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX(final Object v4Id) {
            return g.v(v4Id).out().as("here").has("lang", "java").back("here").value("name");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX(final Object v1Id) {
            return g.v(v1Id).outE().as("here").inV().has("name", "vadas").back("here");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX(final Object v1Id) {
            return g.v(v1Id).outE("knows").has("weight", 1.0d).as("here").inV().has("name", "josh").back("here");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_backXhereX(final Object v1Id) {
            return g.v(v1Id).outE("knows").as("here").has("weight", 1.0d).inV().has("name", "josh").<Edge>back("here");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_backXhereX(final Object v1Id) {
            return g.v(v1Id).outE("knows").as("here").has("weight", 1.0d).as("fake").inV().has("name", "josh").<Edge>back("here");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXhereXout_valueXnameX_backXhereX() {
            return g.V().as("here").out().value("name").back("here");
        }
    }

    public static class ComputerTest extends BackTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_asXhereX_out_backXhereX(final Object v1Id) {
            return g.v(v1Id).as("here").out().<Vertex>back("here").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX(final Object v4Id) {
            return g.v(v4Id).out().as("here").has("lang", "java").<Vertex>back("here").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX(final Object v4Id) {
            return g.v(v4Id).out().as("here").has("lang", "java").back("here").<String>value("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX(final Object v1Id) {
            return g.v(v1Id).outE().as("here").inV().has("name", "vadas").<Edge>back("here").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX(final Object v1Id) {
            return g.v(v1Id).outE("knows").has("weight", 1.0d).as("here").inV().has("name", "josh").<Edge>back("here").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_backXhereX(final Object v1Id) {
            return g.v(v1Id).outE("knows").as("here").has("weight", 1.0d).inV().has("name", "josh").<Edge>back("here").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_backXhereX(final Object v1Id) {
            return g.v(v1Id).outE("knows").as("here").has("weight", 1.0d).as("fake").inV().has("name", "josh").<Edge>back("here").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXhereXout_valueXnameX_backXhereX() {
            return g.V().as("here").out().value("name").<Vertex>back("here").submit(g.compute());
        }
    }
}
