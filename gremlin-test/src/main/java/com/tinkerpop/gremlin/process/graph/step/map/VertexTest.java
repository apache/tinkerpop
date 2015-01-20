package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://traversalhen.genoprime.com)
 * @author Daniel Kuppitz (daniel at thinkaurelius.com)
 */
public abstract class VertexTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V();

    public abstract Traversal<Vertex, Vertex> get_g_v1_out(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_v2_in(final Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_v4_both(final Object v4Id);

    public abstract Traversal<Vertex, String> get_g_v1_localXoutEXknowsX_limitX1XX_inV_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_V_localXbothEXcreatedX_limitX1XX_otherV_name();

    public abstract Traversal<Edge, Edge> get_g_E();

    public abstract Traversal<Vertex, Edge> get_g_v1_outE(final Object v1Id);

    public abstract Traversal<Vertex, Edge> get_g_v2_inE(final Object v2Id);

    public abstract Traversal<Vertex, Edge> get_g_v4_bothE(final Object v4Id);

    public abstract Traversal<Vertex, Edge> get_g_v4_bothEXcreatedX(final Object v4Id);

    public abstract Traversal<Vertex, Edge> get_g_v4_localXbothEX1_createdX_limitX1XX(final Object v4Id);

    public abstract Traversal<Vertex, Edge> get_g_v4_localXbothEXknows_createdX_limitX1XX(final Object v4Id);

    public abstract Traversal<Vertex, String> get_g_v4_localXbothE_limitX1XX_otherV_name(final Object v4Id);

    public abstract Traversal<Vertex, String> get_g_v4_localXbothE_limitX2XX_otherV_name(final Object v4Id);

    public abstract Traversal<Vertex, String> get_g_V_localXinEXknowsX_limitX2XX_outV_name();

    public abstract Traversal<Vertex, Vertex> get_g_v1_outE_inV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_v2_inE_outV(final Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_outE_hasXweight_1X_outV();

    public abstract Traversal<Vertex, String> get_g_V_out_outE_inV_inE_inV_both_name();

    public abstract Traversal<Vertex, String> get_g_v1_outEXknowsX_bothV_name(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_v1_outXknowsX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_v1_outXknows_createdX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_v1_outEXknowsX_inV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_v1_outEXknows_createdX_inV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_out_out();

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_out_out(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_v1_out_name(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_v1_outE_otherV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_v4_bothE_otherV(final Object v4Id);

    public abstract Traversal<Vertex, Vertex> get_g_v4_bothE_hasXweight_lt_1X_otherV(final Object v4Id);

    public abstract Traversal<Vertex, Vertex> get_g_v1_to_XOUT_knowsX(final Object v1Id);

    // VERTEX ADJACENCY

    @Test
    @LoadGraphWith(MODERN)
    public void g_V() {
        final Traversal<Vertex, Vertex> traversal = get_g_V();
        printTraversalForm(traversal);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            vertices.add(traversal.next());
        }
        assertEquals(6, vertices.size());
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_out() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_out(convertToVertexId("marko"));
        assert_g_v1_out(traversal);
    }

    private void assert_g_v1_out(final Traversal<Vertex, Vertex> traversal) {
        printTraversalForm(traversal);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("vadas") ||
                    vertex.value("name").equals("josh") ||
                    vertex.value("name").equals("lop"));
        }
        assertEquals(3, counter);
        assertEquals(3, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v2_in() {
        final Traversal<Vertex, Vertex> traversal = get_g_v2_in(convertToVertexId("vadas"));
        assert_g_v2_in(traversal);
    }

    private void assert_g_v2_in(final Traversal<Vertex, Vertex> traversal) {
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals(traversal.next().value("name"), "marko");
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v4_both() {
        final Traversal<Vertex, Vertex> traversal = get_g_v4_both(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("marko") ||
                    vertex.value("name").equals("ripple") ||
                    vertex.value("name").equals("lop"));
        }
        assertEquals(3, counter);
        assertEquals(3, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_localXoutEXknowsX_limitX1XX_inV_name() {
        final Traversal<Vertex, String> traversal = get_g_v1_localXoutEXknowsX_limitX1XX_inV_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final String name = traversal.next();
        assertTrue(name.equals("vadas") || name.equals("josh"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_localXbothEXcreatedX_limitX1XX_otherV_name() {
        final Traversal<Vertex, String> traversal = get_g_V_localXbothEXcreatedX_limitX1XX_otherV_name();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next();
            assertTrue(name.equals("marko") || name.equals("lop") || name.equals("josh") || name.equals("ripple") || name.equals("peter"));
        }
        assertEquals(5, counter);
    }

    // EDGE ADJACENCY

    @Test
    @LoadGraphWith(MODERN)
    public void g_E() {
        final Traversal<Edge, Edge> traversal = get_g_E();
        printTraversalForm(traversal);
        int counter = 0;
        Set<Edge> edges = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            edges.add(traversal.next());
        }
        assertEquals(6, edges.size());
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outE() {
        final Traversal<Vertex, Edge> traversal = get_g_v1_outE(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        Set<Edge> edges = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            Edge edge = traversal.next();
            edges.add(edge);
            assertTrue(edge.label().equals("knows") || edge.label().equals("created"));
        }
        assertEquals(3, counter);
        assertEquals(3, edges.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v2_inE() {
        final Traversal<Vertex, Edge> traversal = get_g_v2_inE(convertToVertexId("vadas"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals(traversal.next().label(), "knows");
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v4_bothEXcreateX() {
        final Traversal<Vertex, Edge> traversal = get_g_v4_bothEXcreatedX(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        Set<Edge> edges = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            Edge edge = traversal.next();
            edges.add(edge);
            assertTrue(edge.label().equals("created"));
            assertEquals(edge.outV().id().next(), convertToVertexId("josh"));
        }
        assertEquals(2, counter);
        assertEquals(2, edges.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v4_bothE() {
        final Traversal<Vertex, Edge> traversal = get_g_v4_bothE(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        Set<Edge> edges = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            Edge edge = traversal.next();
            edges.add(edge);
            assertTrue(edge.label().equals("knows") || edge.label().equals("created"));
        }
        assertEquals(3, counter);
        assertEquals(3, edges.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v4_localXbothEX1_createdX_limitX1XX() {
        final Traversal<Vertex, Edge> traversal = get_g_v4_localXbothEX1_createdX_limitX1XX(convertToVertexId("josh"));
        printTraversalForm(traversal);
        final Edge edge = traversal.next();
        assertEquals("created", edge.label());
        assertTrue(edge.value("weight").equals(1.0d) || edge.value("weight").equals(0.4d));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v4_localXbothEXknows_createdX_limitX1XX() {
        final Traversal<Vertex, Edge> traversal = get_g_v4_localXbothEXknows_createdX_limitX1XX(convertToVertexId("josh"));
        printTraversalForm(traversal);
        final Edge edge = traversal.next();
        assertTrue(edge.label().equals("created") || edge.label().equals("knows"));
        assertTrue(edge.value("weight").equals(1.0d) || edge.value("weight").equals(0.4d));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v4_localXbothE_limitX1XX_otherV_name() {
        final Traversal<Vertex, String> traversal = get_g_v4_localXbothE_limitX1XX_otherV_name(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next();
            assertTrue(name.equals("marko") || name.equals("ripple") || name.equals("lop"));
        }
        assertEquals(1, counter);
        assertFalse(traversal.hasNext());

    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v4_localXbothE_limitX2XX_otherV_name() {
        final Traversal<Vertex, String> traversal = get_g_v4_localXbothE_limitX2XX_otherV_name(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next();
            assertTrue(name.equals("marko") || name.equals("ripple") || name.equals("lop"));
        }
        assertEquals(2, counter);
        assertFalse(traversal.hasNext());
    }


    @Test
    @LoadGraphWith(MODERN)
    public void g_V_localXinEXknowsX_limitX2XX_outV_name() {
        final Traversal<Vertex, String> traversal = get_g_V_localXinEXknowsX_limitX2XX_outV_name();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals(traversal.next(), "marko");
        }
        assertFalse(traversal.hasNext());
        assertEquals(2, counter);
    }

    // EDGE/VERTEX ADJACENCY

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outE_inV() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_outE_inV(convertToVertexId("marko"));
        this.assert_g_v1_out(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v2_inE_outV() {
        final Traversal<Vertex, Vertex> traversal = get_g_v2_inE_outV(convertToVertexId("vadas"));
        this.assert_g_v2_in(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outE_hasXweight_1X_outV() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_outE_hasXweight_1X_outV();
        printTraversalForm(traversal);
        int counter = 0;
        Map<Object, Integer> counts = new HashMap<>();
        while (traversal.hasNext()) {
            final Object id = traversal.next().id();
            int previousCount = counts.getOrDefault(id, 0);
            counts.put(id, previousCount + 1);
            counter++;
        }
        assertEquals(2, counts.size());
        assertEquals(1, counts.get(convertToVertexId("marko")).intValue());
        assertEquals(1, counts.get(convertToVertexId("josh")).intValue());

        assertEquals(2, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_outE_inV_inE_inV_both_name() {
        final Traversal<Vertex, String> traversal = get_g_V_out_outE_inV_inE_inV_both_name();
        printTraversalForm(traversal);
        int counter = 0;
        Map<String, Integer> counts = new HashMap<>();
        while (traversal.hasNext()) {
            final String key = traversal.next();
            int previousCount = counts.getOrDefault(key, 0);
            counts.put(key, previousCount + 1);
            counter++;
        }
        assertEquals(3, counts.size());
        assertEquals(4, counts.get("josh").intValue());
        assertEquals(3, counts.get("marko").intValue());
        assertEquals(3, counts.get("peter").intValue());

        assertEquals(10, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outEXknowsX_bothV_name() {
        final Traversal<Vertex, String> traversal = get_g_v1_outEXknowsX_bothV_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        List<String> names = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(4, names.size());
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("vadas"));
        names.remove("marko");
        assertEquals(3, names.size());
        names.remove("marko");
        assertEquals(2, names.size());
        names.remove("josh");
        assertEquals(1, names.size());
        names.remove("vadas");
        assertEquals(0, names.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outE_otherV() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_outE_otherV(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("vadas") ||
                    vertex.value("name").equals("josh") ||
                    vertex.value("name").equals("lop"));
        }
        assertEquals(3, counter);
        assertEquals(3, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v4_bothE_otherV() {
        final Traversal<Vertex, Vertex> traversal = get_g_v4_bothE_otherV(convertToVertexId("josh"));
        printTraversalForm(traversal);
        final List<Vertex> vertices = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(3, vertices.size());
        assertTrue(vertices.stream().anyMatch(v -> v.value("name").equals("marko")));
        assertTrue(vertices.stream().anyMatch(v -> v.value("name").equals("ripple")));
        assertTrue(vertices.stream().anyMatch(v -> v.value("name").equals("lop")));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v4_bothE_hasXweight_LT_1X_otherV() {
        final Traversal<Vertex, Vertex> traversal = get_g_v4_bothE_hasXweight_lt_1X_otherV(convertToVertexId("josh"));
        printTraversalForm(traversal);
        final List<Vertex> vertices = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(1, vertices.size());
        assertEquals(vertices.get(0).value("name"), "lop");
        assertFalse(traversal.hasNext());
    }

    // VERTEX EDGE LABEL ADJACENCY

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outXknowsX() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_outXknowsX(convertToVertexId("marko"));
        assert_g_v1_outXknowsX(traversal);
    }

    private void assert_g_v1_outXknowsX(Traversal<Vertex, Vertex> traversal) {
        printTraversalForm(traversal);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("vadas") ||
                    vertex.value("name").equals("josh"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outXknows_createdX() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_outXknows_createdX(convertToVertexId("marko"));
        this.assert_g_v1_out(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outEXknowsX_inV() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_outEXknowsX_inV(convertToVertexId("marko"));
        this.assert_g_v1_outXknowsX(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outEXknows_createdX_inV() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_outEXknows_createdX_inV(convertToVertexId("marko"));
        this.assert_g_v1_out(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_out_out();
        printTraversalForm(traversal);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("lop") ||
                    vertex.value("name").equals("ripple"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_out_out_out() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_out_out_out(convertToVertexId("marko"));
        assertFalse(traversal.hasNext());
    }

    // PROPERTY TESTING

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_out_propertyXnameX() {
        final Traversal<Vertex, String> traversal = get_g_v1_out_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        Set<String> names = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            String name = traversal.next();
            names.add(name);
            assertTrue(name.equals("vadas") ||
                    name.equals("josh") ||
                    name.equals("lop"));
        }
        assertEquals(3, counter);
        assertEquals(3, names.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_to_XOUT_knowsX() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_to_XOUT_knowsX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            String name = vertex.value("name");
            assertTrue(name.equals("vadas") ||
                    name.equals("josh"));
        }
        assertEquals(2, counter);
        assertFalse(traversal.hasNext());

    }

    public static class StandardTest extends VertexTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V() {
            return g.V();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out(final Object v1Id) {
            return g.V(v1Id).out();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v2_in(final Object v2Id) {
            return g.V(v2Id).in();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_both(final Object v4Id) {
            return g.V(v4Id).both();
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_localXoutEXknowsX_limitX1XX_inV_name(final Object v1Id) {
            return g.V(v1Id).local(__.outE("knows").limit(1)).inV().values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXbothEXcreatedX_limitX1XX_otherV_name() {
            return g.V().local(__.bothE("created").limit(1)).otherV().values("name");
        }

        @Override
        public Traversal<Edge, Edge> get_g_E() {
            return g.E();
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outE(final Object v1Id) {
            return g.V(v1Id).outE();
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v2_inE(final Object v2Id) {
            return g.V(v2Id).inE();
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothE(final Object v4Id) {
            return g.V(v4Id).bothE();
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothEXcreatedX(final Object v4Id) {
            return g.V(v4Id).bothE("created");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_localXbothEX1_createdX_limitX1XX(final Object v4Id) {
            return g.V(v4Id).local(__.bothE("created").limit(1));
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_localXbothEXknows_createdX_limitX1XX(final Object v4Id) {
            return g.V(v4Id).local(__.bothE("knows", "created").limit(1));
        }

        @Override
        public Traversal<Vertex, String> get_g_v4_localXbothE_limitX1XX_otherV_name(final Object v4Id) {
            return g.V(v4Id).local(__.bothE().limit(1)).otherV().values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_v4_localXbothE_limitX2XX_otherV_name(final Object v4Id) {
            return g.V(v4Id).local(__.bothE().limit(2)).otherV().values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXinEXknowsX_limitX2XX_outV_name() {
            return g.V().local(__.inE("knows").limit(2)).outV().values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outE_inV(final Object v1Id) {
            return g.V(v1Id).outE().inV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v2_inE_outV(final Object v2Id) {
            return g.V(v2Id).inE().outV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_outE_hasXweight_1X_outV() {
            return g.V().outE().has("weight", 1.0d).outV();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_outE_inV_inE_inV_both_name() {
            return g.V().out().outE().inV().inE().inV().both().values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_outEXknowsX_bothV_name(final Object v1Id) {
            return g.V(v1Id).outE("knows").bothV().values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXknowsX(final Object v1Id) {
            return g.V(v1Id).out("knows");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXknows_createdX(final Object v1Id) {
            return g.V(v1Id).out("knows", "created");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outEXknowsX_inV(final Object v1Id) {
            return g.V(v1Id).outE("knows").inV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outEXknows_createdX_inV(final Object v1Id) {
            return g.V(v1Id).outE("knows", "created").inV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outE_otherV(final Object v1Id) {
            return g.V(v1Id).outE().otherV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothE_otherV(final Object v4Id) {
            return g.V(v4Id).bothE().otherV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothE_hasXweight_lt_1X_otherV(Object v4Id) {
            return g.V(v4Id).bothE().has("weight", Compare.lt, 1d).otherV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_out() {
            return g.V().out().out();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_out_out(final Object v1Id) {
            return g.V(v1Id).out().out().out();
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_out_name(final Object v1Id) {
            return g.V(v1Id).out().values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_to_XOUT_knowsX(final Object v1Id) {
            return g.V(v1Id).to(Direction.OUT, "knows");
        }
    }

    public static class ComputerTest extends VertexTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V() {
            return g.V().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out(final Object v1Id) {
            return g.V(v1Id).out().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v2_in(final Object v2Id) {
            return g.V(v2Id).in().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_both(final Object v4Id) {
            return g.V(v4Id).both().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_localXoutEXknowsX_limitX1XX_inV_name(final Object v1Id) {
            return g.V(v1Id).local(__.outE("knows").limit(1)).inV().<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXbothEXcreatedX_limitX1XX_otherV_name() {
            return g.V().local(__.bothE("created").limit(1)).otherV().<String>values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_v4_localXbothE_limitX1XX_otherV_name(final Object v4Id) {
            return g.V(v4Id).local(__.bothE().limit(1)).otherV().<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_v4_localXbothE_limitX2XX_otherV_name(final Object v4Id) {
            return g.V(v4Id).local(__.bothE().limit(2)).otherV().<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Edge, Edge> get_g_E() {
            return g.E().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outE(final Object v1Id) {
            return g.V(v1Id).outE().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v2_inE(final Object v2Id) {
            return g.V(v2Id).inE().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothE(final Object v4Id) {
            return g.V(v4Id).bothE().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothEXcreatedX(final Object v4Id) {
            return g.V(v4Id).bothE("created").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_localXbothEX1_createdX_limitX1XX(final Object v4Id) {
            return g.V(v4Id).local(__.bothE("created").limit(1)).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_localXbothEXknows_createdX_limitX1XX(final Object v4Id) {
            return g.V(v4Id).local(__.bothE("knows", "created").limit(1)).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXinEXknowsX_limitX2XX_outV_name() {
            return g.V().local(__.inE("knows").limit(2)).outV().<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outE_inV(final Object v1Id) {
            return g.V(v1Id).outE().inV().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v2_inE_outV(final Object v2Id) {
            return g.V(v2Id).inE().outV().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_outE_hasXweight_1X_outV() {
            return g.V().outE().has("weight", 1.0d).outV().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_outE_inV_inE_inV_both_name() {
            return g.V().out().outE().inV().inE().inV().both().<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_outEXknowsX_bothV_name(final Object v1Id) {
            return g.V(v1Id).outE("knows").bothV().<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXknowsX(final Object v1Id) {
            return g.V(v1Id).out("knows").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXknows_createdX(final Object v1Id) {
            return g.V(v1Id).out("knows", "created").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outEXknowsX_inV(final Object v1Id) {
            return g.V(v1Id).outE("knows").inV().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outEXknows_createdX_inV(final Object v1Id) {
            return g.V(v1Id).outE("knows", "created").inV().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outE_otherV(final Object v1Id) {
            return g.V(v1Id).outE().otherV().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothE_otherV(final Object v4Id) {
            return g.V(v4Id).bothE().otherV().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothE_hasXweight_lt_1X_otherV(Object v4Id) {
            return g.V(v4Id).bothE().has("weight", Compare.lt, 1d).otherV().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_out() {
            return g.V().out().out().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_out_out(final Object v1Id) {
            return g.V(v1Id).out().out().out().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_out_name(final Object v1Id) {
            return g.V(v1Id).out().<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_to_XOUT_knowsX(final Object v1Id) {
            return g.V(v1Id).to(Direction.OUT, "knows").submit(g.compute());
        }
    }
}
