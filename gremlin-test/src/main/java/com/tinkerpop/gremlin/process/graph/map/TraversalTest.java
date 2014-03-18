package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class TraversalTest extends AbstractGremlinProcessTest {

    public abstract Iterator<Vertex> get_g_V();

    public abstract Iterator<Vertex> get_g_v1_out(final Object v1Id);

    public abstract Iterator<Vertex> get_g_v2_in(final Object v2Id);

    public abstract Iterator<Vertex> get_g_v4_both(final Object v4Id);

    public abstract Iterator<String> get_g_v1_outX1_knowsX_name(final Object v1Id);

    public abstract Iterator<String> get_g_V_bothX1_createdX_name();

    public abstract Iterator<Edge> get_g_E();

    public abstract Iterator<Edge> get_g_v1_outE(final Object v1Id);

    public abstract Iterator<Edge> get_g_v2_inE(final Object v2Id);

    public abstract Iterator<Edge> get_g_v4_bothE(final Object v4Id);

    public abstract Iterator<Edge> get_g_v4_bothEX1_createdX(final Object v4Id);

    public abstract Iterator<String> get_g_V_inEX2_knowsX_outV_name();

    public abstract Iterator<Vertex> get_g_v1_outE_inV(final Object v1Id);

    public abstract Iterator<Vertex> get_g_v2_inE_outV(final Object v2Id);

    public abstract Iterator<Vertex> get_g_V_outE_hasXweight_1X_outV();

    public abstract Iterator<String> get_g_V_out_outE_inV_inE_inV_both_name();

    public abstract Iterator<String> get_g_v1_outEXknowsX_bothV_name(final Object v1Id);

    public abstract Iterator<Vertex> get_g_v1_outXknowsX(final Object v1Id);

    public abstract Iterator<Vertex> get_g_v1_outXknows_createdX(final Object v1Id);

    public abstract Iterator<Vertex> get_g_v1_outEXknowsX_inV(final Object v1Id);

    public abstract Iterator<Vertex> get_g_v1_outEXknows_createdX_inV(final Object v1Id);

    public abstract Iterator<Vertex> get_g_V_out_out();

    public abstract Iterator<Vertex> get_g_v1_out_out_out(final Object v1Id);

    public abstract Iterator<String> get_g_v1_out_propertyXnameX(final Object v1Id);

    // VERTEX ADJACENCY

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V() {
        final Iterator<Vertex> step = get_g_V();
        System.out.println("Testing: " + step);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (step.hasNext()) {
            counter++;
            vertices.add(step.next());
        }
        assertEquals(6, vertices.size());
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out() {
        final Iterator<Vertex> step = get_g_v1_out(convertToId("marko"));
        assert_g_v1_out(step);
    }

    private void assert_g_v1_out(final Iterator<Vertex> step) {
        System.out.println("Testing: " + step);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (step.hasNext()) {
            counter++;
            Vertex vertex = step.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("vadas") ||
                    vertex.getValue("name").equals("josh") ||
                    vertex.getValue("name").equals("lop"));
        }
        assertEquals(3, counter);
        assertEquals(3, vertices.size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v2_in() {
        final Iterator<Vertex> step = get_g_v2_in(convertToId("vadas"));
        assert_g_v2_in(step);
    }

    private void assert_g_v2_in(final Iterator<Vertex> step) {
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            assertEquals(step.next().getValue("name"), "marko");
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v4_both() {
        final Iterator<Vertex> step = get_g_v4_both(convertToId("josh"));
        System.out.println("Testing: " + step);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (step.hasNext()) {
            counter++;
            Vertex vertex = step.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("marko") ||
                    vertex.getValue("name").equals("ripple") ||
                    vertex.getValue("name").equals("lop"));
        }
        assertEquals(3, counter);
        assertEquals(3, vertices.size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outX1_knowsX_name() {
        final Iterator<String> step = get_g_v1_outX1_knowsX_name(convertToId("marko"));
        System.out.println("Testing: " + step);
        final String name = step.next();
        assertTrue(name.equals("vadas") || name.equals("josh"));
        assertFalse(step.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_bothX1_createdX_name() {
        final Iterator<String> step = get_g_V_bothX1_createdX_name();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            final String name = step.next();
            assertTrue(name.equals("marko") || name.equals("lop") || name.equals("josh") || name.equals("ripple") || name.equals("peter"));
        }
        assertEquals(5, counter);
    }

    // EDGE ADJACENCY

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_E() {
        final Iterator<Edge> step = get_g_E();
        System.out.println("Testing: " + step);
        int counter = 0;
        Set<Edge> edges = new HashSet<>();
        while (step.hasNext()) {
            counter++;
            edges.add(step.next());
        }
        assertEquals(6, edges.size());
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outE() {
        final Iterator<Edge> step = get_g_v1_outE(convertToId("marko"));
        System.out.println("Testing: " + step);
        int counter = 0;
        Set<Edge> edges = new HashSet<>();
        while (step.hasNext()) {
            counter++;
            Edge edge = step.next();
            edges.add(edge);
            assertTrue(edge.getLabel().equals("knows") || edge.getLabel().equals("created"));
        }
        assertEquals(3, counter);
        assertEquals(3, edges.size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v2_inE() {
        final Iterator<Edge> step = get_g_v2_inE(convertToId("vadas"));
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            assertEquals(step.next().getLabel(), "knows");
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v4_bothE() {
        final Iterator<Edge> step = get_g_v4_bothE(convertToId("josh"));
        System.out.println("Testing: " + step);
        int counter = 0;
        Set<Edge> edges = new HashSet<>();
        while (step.hasNext()) {
            counter++;
            Edge edge = step.next();
            edges.add(edge);
            assertTrue(edge.getLabel().equals("knows") || edge.getLabel().equals("created"));
        }
        assertEquals(3, counter);
        assertEquals(3, edges.size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v4_bothEX1_createdX() {
        final Iterator<Edge> step = get_g_v4_bothEX1_createdX(convertToId("josh"));
        System.out.println("Testing: " + step);
        final Edge edge = step.next();
        assertEquals("created", edge.getLabel());
        assertTrue(edge.getValue("weight").equals(1.0f) || edge.getValue("weight").equals(0.4f));
        assertFalse(step.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_inEX2_knowsX_outV_name() {
        final Iterator<String> step = get_g_V_inEX2_knowsX_outV_name();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            assertEquals(step.next(), "marko");
        }
        assertFalse(step.hasNext());
        assertEquals(2, counter);
    }

    // EDGE/VERTEX ADJACENCY

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outE_inV() {
        final Iterator<Vertex> step = get_g_v1_outE_inV(convertToId("marko"));
        this.assert_g_v1_out(step);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v2_inE_outV() {
        final Iterator<Vertex> step = get_g_v2_inE_outV(convertToId("vadas"));
        this.assert_g_v2_in(step);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_outE_hasXweight_1X_outV() {
        final Iterator<Vertex> step = get_g_V_outE_hasXweight_1X_outV();
        System.out.println("Testing: " + step);
        int counter = 0;
        Map<Object, Integer> counts = new HashMap<>();
        while (step.hasNext()) {
            final Object id = step.next().getId();
            int previousCount = counts.getOrDefault(id, 0);
            counts.put(id, previousCount + 1);
            counter++;
        }
        assertEquals(2, counts.size());
        assertEquals(1, counts.get(convertToId("marko")).intValue());
        assertEquals(1, counts.get(convertToId("josh")).intValue());

        assertEquals(2, counter);
        assertFalse(step.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_out_outE_inV_inE_inV_both_name() {
        final Iterator<String> step = get_g_V_out_outE_inV_inE_inV_both_name();
        System.out.println("Testing: " + step);
        int counter = 0;
        Map<String, Integer> counts = new HashMap<>();
        while (step.hasNext()) {
            final String key = step.next();
            int previousCount = counts.getOrDefault(key, 0);
            counts.put(key, previousCount + 1);
            counter++;
        }
        assertEquals(3, counts.size());
        assertEquals(4, counts.get("josh").intValue());
        assertEquals(3, counts.get("marko").intValue());
        assertEquals(3, counts.get("peter").intValue());

        assertEquals(10, counter);
        assertFalse(step.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outEXknowsX_bothV_name() {
        final Iterator<String> step = get_g_v1_outEXknowsX_bothV_name(convertToId("marko"));
        System.out.println("Testing: " + step);
        List<String> names = StreamFactory.stream(step).collect(Collectors.toList());
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

    // VERTEX EDGE LABEL ADJACENCY

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outXknowsX() {
        final Iterator<Vertex> step = get_g_v1_outXknowsX(convertToId("marko"));
        assert_g_v1_outXknowsX(step);
    }

    private void assert_g_v1_outXknowsX(Iterator<Vertex> step) {
        System.out.println("Testing: " + step);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (step.hasNext()) {
            counter++;
            Vertex vertex = step.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("vadas") ||
                    vertex.getValue("name").equals("josh"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outXknows_createdX() {
        final Iterator<Vertex> step = get_g_v1_outXknows_createdX(convertToId("marko"));
        this.assert_g_v1_out(step);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outEXknowsX_inV() {
        final Iterator<Vertex> step = get_g_v1_outEXknowsX_inV(convertToId("marko"));
        this.assert_g_v1_outXknowsX(step);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outEXknows_createdX_inV() {
        final Iterator<Vertex> step = get_g_v1_outEXknows_createdX_inV(convertToId("marko"));
        this.assert_g_v1_out(step);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_out_out() {
        final Iterator<Vertex> step = get_g_V_out_out();
        System.out.println("Testing: " + step);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (step.hasNext()) {
            counter++;
            Vertex vertex = step.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("lop") ||
                    vertex.getValue("name").equals("ripple"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_out_out() {
        final Iterator<Vertex> step = get_g_v1_out_out_out(convertToId("marko"));
        assertFalse(step.hasNext());
    }

    // PROPERTY TESTING

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_propertyXnameX() {
        final Iterator<String> step = get_g_v1_out_propertyXnameX(convertToId("marko"));
        System.out.println("Testing: " + step);
        int counter = 0;
        Set<String> names = new HashSet<>();
        while (step.hasNext()) {
            counter++;
            String name = step.next();
            names.add(name);
            assertTrue(name.equals("vadas") ||
                    name.equals("josh") ||
                    name.equals("lop"));
        }
        assertEquals(3, counter);
        assertEquals(3, names.size());
    }

    public static class JavaTraversalTest extends TraversalTest {
        public JavaTraversalTest() {
            requiresGraphComputer = false;
        }

        public Iterator<Vertex> get_g_V() {
            return g.V();
        }

        public Iterator<Vertex> get_g_v1_out(final Object v1Id) {
            return g.v(v1Id).out();
        }

        public Iterator<Vertex> get_g_v2_in(final Object v2Id) {
            return g.v(v2Id).in();
        }

        public Iterator<Vertex> get_g_v4_both(final Object v4Id) {
            return g.v(v4Id).both();
        }

        public Iterator<String> get_g_v1_outX1_knowsX_name(final Object v1Id) {
            return g.v(v1Id).out(1, "knows").value("name");
        }

        public Iterator<String> get_g_V_bothX1_createdX_name() {
            return g.V().both(1, "created").value("name");
        }

        public Iterator<Edge> get_g_E() {
            return g.E();
        }

        public Iterator<Edge> get_g_v1_outE(final Object v1Id) {
            return g.v(v1Id).outE();
        }

        public Iterator<Edge> get_g_v2_inE(final Object v2Id) {
            return g.v(v2Id).inE();
        }

        public Iterator<Edge> get_g_v4_bothE(final Object v4Id) {
            return g.v(v4Id).bothE();
        }

        public Iterator<Edge> get_g_v4_bothEX1_createdX(final Object v4Id) {
            return g.v(v4Id).bothE(1, "created");
        }

        public Iterator<String> get_g_V_inEX2_knowsX_outV_name() {
            return g.V().inE(2, "knows").outV().value("name");
        }

        public Iterator<Vertex> get_g_v1_outE_inV(final Object v1Id) {
            return g.v(v1Id).outE().inV();
        }

        public Iterator<Vertex> get_g_v2_inE_outV(final Object v2Id) {
            return g.v(v2Id).inE().outV();
        }

        public Iterator<Vertex> get_g_V_outE_hasXweight_1X_outV() {
            return g.V().outE().has("weight", 1.0f).outV();
        }

        public Iterator<String> get_g_V_out_outE_inV_inE_inV_both_name() {
            return g.V().out().outE().inV().inE().inV().both().value("name");
        }

        public Iterator<String> get_g_v1_outEXknowsX_bothV_name(final Object v1Id) {
            return g.v(v1Id).outE("knows").bothV().value("name");
        }

        public Iterator<Vertex> get_g_v1_outXknowsX(final Object v1Id) {
            return g.v(v1Id).out("knows");
        }

        public Iterator<Vertex> get_g_v1_outXknows_createdX(final Object v1Id) {
            return g.v(v1Id).out("knows", "created");
        }

        public Iterator<Vertex> get_g_v1_outEXknowsX_inV(final Object v1Id) {
            return g.v(v1Id).outE("knows").inV();
        }

        public Iterator<Vertex> get_g_v1_outEXknows_createdX_inV(final Object v1Id) {
            return g.v(v1Id).outE("knows", "created").inV();
        }

        public Iterator<Vertex> get_g_V_out_out() {
            return g.V().out().out();
        }

        public Iterator<Vertex> get_g_v1_out_out_out(final Object v1Id) {
            return g.v(v1Id).out().out().out();
        }

        public Iterator<String> get_g_v1_out_propertyXnameX(final Object v1Id) {
            return g.v(v1Id).out().value("name");
        }
    }

    // todo: some of the graph computer tests do not pass

    public static class JavaComputerTraversalTest extends TraversalTest {
        public JavaComputerTraversalTest() {
            requiresGraphComputer = true;
        }

        public Iterator<Vertex> get_g_V() {
            return g.V().submit(g.compute());
        }

        public Iterator<Vertex> get_g_v1_out(final Object v1Id) {
            return g.v(v1Id).out(); //.submit(g.compute());
        }

        public Iterator<Vertex> get_g_v2_in(final Object v2Id) {
            return g.v(v2Id).in();  //.submit(g.compute());
        }

        public Iterator<Vertex> get_g_v4_both(final Object v4Id) {
            return g.v(v4Id).both(); // .submit(g.compute());
        }

        public Iterator<String> get_g_v1_outX1_knowsX_name(final Object v1Id) {
            return g.v(v1Id).out(1, "knows").value("name");  // .submit(g.compute());
        }

        public Iterator<String> get_g_V_bothX1_createdX_name() {
            return g.V().both(1, "created").<String>value("name").submit(g.compute());
        }

        public Iterator<Edge> get_g_E() {
            return g.E().submit(g.compute());
        }

        public Iterator<Edge> get_g_v1_outE(final Object v1Id) {
            return g.v(v1Id).outE(); //.submit(g.compute());
        }

        public Iterator<Edge> get_g_v2_inE(final Object v2Id) {
            return g.v(v2Id).inE(); // .submit(g.compute());
        }

        public Iterator<Edge> get_g_v4_bothE(final Object v4Id) {
            return g.v(v4Id).bothE();  // .submit(g.compute());
        }

        public Iterator<Edge> get_g_v4_bothEX1_createdX(final Object v4Id) {
            return g.v(v4Id).bothE(1, "created"); //.submit(g.compute());
        }

        public Iterator<String> get_g_V_inEX2_knowsX_outV_name() {
            return g.V().inE(2, "knows").outV().<String>value("name").submit(g.compute());
        }

        public Iterator<Vertex> get_g_v1_outE_inV(final Object v1Id) {
            return g.v(v1Id).outE().inV(); //.submit(g.compute());
        }

        public Iterator<Vertex> get_g_v2_inE_outV(final Object v2Id) {
            return g.v(v2Id).inE().outV(); // .submit(g.compute());
        }

        public Iterator<Vertex> get_g_V_outE_hasXweight_1X_outV() {
            return g.V().outE().has("weight", 1.0f).outV().submit(g.compute());
        }

        public Iterator<String> get_g_V_out_outE_inV_inE_inV_both_name() {
            return g.V().out().outE().inV().inE().inV().both().<String>value("name").submit(g.compute());
        }

        public Iterator<String> get_g_v1_outEXknowsX_bothV_name(final Object v1Id) {
            return g.v(v1Id).outE("knows").bothV().value("name"); //.submit(g.compute());
        }

        public Iterator<Vertex> get_g_v1_outXknowsX(final Object v1Id) {
            return g.v(v1Id).out("knows");  //.submit(g.compute());
        }

        public Iterator<Vertex> get_g_v1_outXknows_createdX(final Object v1Id) {
            return g.v(v1Id).out("knows", "created"); //.submit(g.compute());
        }

        public Iterator<Vertex> get_g_v1_outEXknowsX_inV(final Object v1Id) {
            return g.v(v1Id).outE("knows").inV(); //.submit(g.compute());
        }

        public Iterator<Vertex> get_g_v1_outEXknows_createdX_inV(final Object v1Id) {
            return g.v(v1Id).outE("knows", "created").inV();  //.submit(g.compute());
        }

        public Iterator<Vertex> get_g_V_out_out() {
            return g.V().out().out().submit(g.compute());
        }

        public Iterator<Vertex> get_g_v1_out_out_out(final Object v1Id) {
            return g.v(v1Id).out().out().out(); //.submit(g.compute());
        }

        public Iterator<String> get_g_v1_out_propertyXnameX(final Object v1Id) {
            return g.v(v1Id).out().value("name"); //.submit(g.compute());
        }
    }
}
