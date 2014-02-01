package com.tinkerpop.gremlin.test.map;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalTest {

    public void testCompliance() {
        assertTrue(true);
    }

    // VERTEX ADJACENCY

    public void g_V(final Iterator<Vertex> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (pipe.hasNext()) {
            counter++;
            vertices.add(pipe.next());
        }
        assertEquals(6, vertices.size());
        assertEquals(6, counter);
    }

    public void g_v1_out(final Iterator<Vertex> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (pipe.hasNext()) {
            counter++;
            Vertex vertex = pipe.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("vadas") ||
                    vertex.getValue("name").equals("josh") ||
                    vertex.getValue("name").equals("lop"));
        }
        assertEquals(3, counter);
        assertEquals(3, vertices.size());
    }

    public void g_v2_in(final Iterator<Vertex> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            assertEquals(pipe.next().getValue("name"), "marko");
        }
        assertEquals(1, counter);
    }

    public void g_v4_both(final Iterator<Vertex> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (pipe.hasNext()) {
            counter++;
            Vertex vertex = pipe.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("marko") ||
                    vertex.getValue("name").equals("ripple") ||
                    vertex.getValue("name").equals("lop"));
        }
        assertEquals(3, counter);
        assertEquals(3, vertices.size());
    }

    public void g_v1_outX1_knowsX_name(final Iterator<String> pipe) {
        System.out.println("Testing: " + pipe);
        final String name = pipe.next();
        assertTrue(name.equals("vadas") || name.equals("josh"));
        assertFalse(pipe.hasNext());
    }

    public void g_V_bothX1_createdX_name(final Iterator<String> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            final String name = pipe.next();
            assertTrue(name.equals("marko") || name.equals("lop") || name.equals("josh") || name.equals("ripple") || name.equals("peter"));
        }
        assertEquals(5, counter);
    }

    // EDGE ADJACENCY

    public void g_E(final Iterator<Edge> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        Set<Edge> edges = new HashSet<>();
        while (pipe.hasNext()) {
            counter++;
            edges.add(pipe.next());
        }
        assertEquals(6, edges.size());
        assertEquals(6, counter);
    }

    public void g_v1_outE(final Iterator<Edge> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        Set<Edge> edges = new HashSet<>();
        while (pipe.hasNext()) {
            counter++;
            Edge edge = pipe.next();
            edges.add(edge);
            assertTrue(edge.getLabel().equals("knows") || edge.getLabel().equals("created"));
        }
        assertEquals(3, counter);
        assertEquals(3, edges.size());
    }

    public void g_v2_inE(final Iterator<Edge> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            assertEquals(pipe.next().getLabel(), "knows");
        }
        assertEquals(1, counter);
    }

    public void g_v4_bothE(final Iterator<Edge> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        Set<Edge> edges = new HashSet<>();
        while (pipe.hasNext()) {
            counter++;
            Edge edge = pipe.next();
            edges.add(edge);
            assertTrue(edge.getLabel().equals("knows") || edge.getLabel().equals("created"));
        }
        assertEquals(3, counter);
        assertEquals(3, edges.size());
    }

    public void g_v4_bothEX1_createdX(final Iterator<Edge> pipe) {
        System.out.println("Testing: " + pipe);
        final Edge edge = pipe.next();
        assertEquals("created", edge.getLabel());
        assertTrue(edge.getValue("weight").equals(1.0f) || edge.getValue("weight").equals(0.4f));
        assertFalse(pipe.hasNext());
    }

    public void g_V_inEX2_knowsX_outV_name(final Iterator<String> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            assertEquals(pipe.next(), "marko");
        }
        assertFalse(pipe.hasNext());
        assertEquals(2, counter);
    }

    // EDGE/VERTEX ADJACENCY

    public void g_v1_outE_inV(final Iterator<Vertex> pipe) {
        this.g_v1_out(pipe);
    }

    public void g_v2_inE_outV(final Iterator<Vertex> pipe) {
        this.g_v2_in(pipe);
    }

    public void g_V_outE_hasXweight_1X_outV(final Iterator<Vertex> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        Map<Object, Integer> counts = new HashMap<>();
        while (pipe.hasNext()) {
            final Object id = pipe.next().getId();
            int previousCount = counts.getOrDefault(id, 0);
            counts.put(id, previousCount + 1);
            counter++;
        }
        assertEquals(2, counts.size());
        assertEquals(1, counts.get("1").intValue());
        assertEquals(1, counts.get("4").intValue());

        assertEquals(2, counter);
        assertFalse(pipe.hasNext());
    }

    public void g_V_out_outE_inV_inE_inV_both_name(final Iterator<String> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        Map<String, Integer> counts = new HashMap<>();
        while (pipe.hasNext()) {
            final String key = pipe.next();
            int previousCount = counts.getOrDefault(key, 0);
            counts.put(key, previousCount + 1);
            counter++;
        }
        assertEquals(3, counts.size());
        assertEquals(4, counts.get("josh").intValue());
        assertEquals(3, counts.get("marko").intValue());
        assertEquals(3, counts.get("peter").intValue());

        assertEquals(10, counter);
        assertFalse(pipe.hasNext());
    }

    public void g_v1_outEXknowsX_bothV_name(final Iterator<String> pipe) {
        System.out.println("Testing: " + pipe);
        List<String> names = StreamFactory.stream(pipe).collect(Collectors.toList());
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

    public void g_v1_outXknowsX(final Iterator<Vertex> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (pipe.hasNext()) {
            counter++;
            Vertex vertex = pipe.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("vadas") ||
                    vertex.getValue("name").equals("josh"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    public void g_v1_outXknows_createdX(final Iterator<Vertex> pipe) {
        this.g_v1_out(pipe);
    }

    public void g_v1_outEXknowsX_inV(final Iterator<Vertex> pipe) {
        this.g_v1_outXknowsX(pipe);
    }

    public void g_v1_outEXknows_createdX_inV(final Iterator<Vertex> pipe) {
        this.g_v1_outE_inV(pipe);
    }

    public void g_V_out_out(final Iterator<Vertex> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (pipe.hasNext()) {
            counter++;
            Vertex vertex = pipe.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("lop") ||
                    vertex.getValue("name").equals("ripple"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    public void g_v1_out_out_out(final Iterator<Vertex> pipe) {
        assertFalse(pipe.hasNext());
    }

    // PROPERTY TESTING

    public void g_v1_out_propertyXnameX(final Iterator<String> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        Set<String> names = new HashSet<>();
        while (pipe.hasNext()) {
            counter++;
            String name = pipe.next();
            names.add(name);
            assertTrue(name.equals("vadas") ||
                    name.equals("josh") ||
                    name.equals("lop"));
        }
        assertEquals(3, counter);
        assertEquals(3, names.size());
    }
}
