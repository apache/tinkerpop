package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StreamFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalTest {

    public void testCompliance() {
        assertTrue(true);
    }

    // VERTEX ADJACENCY

    public void g_V(final Iterator<Vertex> step) {
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

    public void g_v1_out(final Iterator<Vertex> step) {
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

    public void g_v2_in(final Iterator<Vertex> step) {
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            assertEquals(step.next().getValue("name"), "marko");
        }
        assertEquals(1, counter);
    }

    public void g_v4_both(final Iterator<Vertex> step) {
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

    public void g_v1_outX1_knowsX_name(final Iterator<String> step) {
        System.out.println("Testing: " + step);
        final String name = step.next();
        assertTrue(name.equals("vadas") || name.equals("josh"));
        assertFalse(step.hasNext());
    }

    public void g_V_bothX1_createdX_name(final Iterator<String> step) {
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

    public void g_E(final Iterator<Edge> step) {
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

    public void g_v1_outE(final Iterator<Edge> step) {
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

    public void g_v2_inE(final Iterator<Edge> step) {
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            assertEquals(step.next().getLabel(), "knows");
        }
        assertEquals(1, counter);
    }

    public void g_v4_bothE(final Iterator<Edge> step) {
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

    public void g_v4_bothEX1_createdX(final Iterator<Edge> step) {
        System.out.println("Testing: " + step);
        final Edge edge = step.next();
        assertEquals("created", edge.getLabel());
        assertTrue(edge.getValue("weight").equals(1.0f) || edge.getValue("weight").equals(0.4f));
        assertFalse(step.hasNext());
    }

    public void g_V_inEX2_knowsX_outV_name(final Iterator<String> step) {
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

    public void g_v1_outE_inV(final Iterator<Vertex> step) {
        this.g_v1_out(step);
    }

    public void g_v2_inE_outV(final Iterator<Vertex> step) {
        this.g_v2_in(step);
    }

    public void g_V_outE_hasXweight_1X_outV(final Iterator<Vertex> step) {
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
        assertEquals(1, counts.get("1").intValue());
        assertEquals(1, counts.get("4").intValue());

        assertEquals(2, counter);
        assertFalse(step.hasNext());
    }

    public void g_V_out_outE_inV_inE_inV_both_name(final Iterator<String> step) {
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

    public void g_v1_outEXknowsX_bothV_name(final Iterator<String> step) {
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

    public void g_v1_outXknowsX(final Iterator<Vertex> step) {
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

    public void g_v1_outXknows_createdX(final Iterator<Vertex> step) {
        this.g_v1_out(step);
    }

    public void g_v1_outEXknowsX_inV(final Iterator<Vertex> step) {
        this.g_v1_outXknowsX(step);
    }

    public void g_v1_outEXknows_createdX_inV(final Iterator<Vertex> step) {
        this.g_v1_outE_inV(step);
    }

    public void g_V_out_out(final Iterator<Vertex> step) {
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

    public void g_v1_out_out_out(final Iterator<Vertex> step) {
        assertFalse(step.hasNext());
    }

    // PROPERTY TESTING

    public void g_v1_out_propertyXnameX(final Iterator<String> step) {
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
}
