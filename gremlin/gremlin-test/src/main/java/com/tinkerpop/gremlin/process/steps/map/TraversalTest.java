package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
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
public abstract class TraversalTest extends AbstractGremlinTest {

    public abstract Iterator<Vertex> get_g_V();

    public abstract Iterator<Vertex> get_g_v1_out();

    public abstract Iterator<Vertex> get_g_v2_in();

    public abstract Iterator<Vertex> get_g_v4_both();

    public abstract Iterator<String> get_g_v1_outX1_knowsX_name();

    public abstract Iterator<String> get_g_V_bothX1_createdX_name();

    public abstract Iterator<Edge> get_g_E();

    public abstract Iterator<Edge> get_g_v1_outE();

    public abstract Iterator<Edge> get_g_v2_inE();

    public abstract Iterator<Edge> get_g_v4_bothE();

    public abstract Iterator<Edge> get_g_v4_bothEX1_createdX();

    public abstract Iterator<String> get_g_V_inEX2_knowsX_outV_name();

    public abstract Iterator<Vertex> get_g_v1_outE_inV();

    public abstract Iterator<Vertex> get_g_v2_inE_outV();

    public abstract Iterator<Vertex> get_g_V_outE_hasXweight_1X_outV();

    public abstract Iterator<String> get_g_V_out_outE_inV_inE_inV_both_name();

    public abstract Iterator<String> get_g_v1_outEXknowsX_bothV_name();

    public abstract Iterator<Vertex> get_g_v1_outXknowsX();

    public abstract Iterator<Vertex> get_g_v1_outXknows_createdX();

    public abstract Iterator<Vertex> get_g_v1_outEXknowsX_inV();

    public abstract Iterator<Vertex> get_g_v1_outEXknows_createdX_inV();

    public abstract Iterator<Vertex> get_g_V_out_out();

    public abstract Iterator<Vertex> get_g_v1_out_out_out();

    public abstract Iterator<String> get_g_v1_out_propertyXnameX();

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
        final Iterator<Vertex> step = get_g_v1_out();
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
        final Iterator<Vertex> step = get_g_v2_in();
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
        final Iterator<Vertex> step = get_g_v4_both();
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
        final Iterator<String> step = get_g_v1_outX1_knowsX_name();
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
        final Iterator<Edge> step = get_g_v1_outE();
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
        final Iterator<Edge> step = get_g_v2_inE();
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
        final Iterator<Edge> step = get_g_v4_bothE();
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
        final Iterator<Edge> step = get_g_v4_bothEX1_createdX();
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
        final Iterator<Vertex> step = get_g_v1_outE_inV();
        this.assert_g_v1_out(step);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v2_inE_outV() {
        final Iterator<Vertex> step = get_g_v2_inE_outV();
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
        assertEquals(1, counts.get("1").intValue());
        assertEquals(1, counts.get("4").intValue());

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
        final Iterator<String> step = get_g_v1_outEXknowsX_bothV_name();
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
        final Iterator<Vertex> step = get_g_v1_outXknowsX();
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
        final Iterator<Vertex> step = get_g_v1_outXknows_createdX();
        this.assert_g_v1_out(step);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outEXknowsX_inV() {
        final Iterator<Vertex> step = get_g_v1_outEXknowsX_inV();
        this.assert_g_v1_outXknowsX(step);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outEXknows_createdX_inV() {
        final Iterator<Vertex> step = get_g_v1_outEXknows_createdX_inV();
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
        final Iterator<Vertex> step = get_g_v1_out_out_out();
        assertFalse(step.hasNext());
    }

    // PROPERTY TESTING

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_propertyXnameX() {
        final Iterator<String> step = get_g_v1_out_propertyXnameX();
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
        public Iterator<Vertex> get_g_V() {
            return g.V();
        }

        public Iterator<Vertex> get_g_v1_out() {
            return g.v(1).out();
        }

        public Iterator<Vertex> get_g_v2_in() {
            return g.v(2).in();
        }

        public Iterator<Vertex> get_g_v4_both() {
            return g.v(4).both();
        }

        public Iterator<String> get_g_v1_outX1_knowsX_name() {
            return g.v(1).out(1, "knows").value("name");
        }

        public Iterator<String> get_g_V_bothX1_createdX_name() {
            return g.V().both(1, "created").value("name");
        }

        public Iterator<Edge> get_g_E() {
            return g.E();
        }

        public Iterator<Edge> get_g_v1_outE() {
            return g.v(1).outE();
        }

        public Iterator<Edge> get_g_v2_inE() {
            return g.v(2).inE();
        }

        public Iterator<Edge> get_g_v4_bothE() {
            return g.v(4).bothE();
        }

        public Iterator<Edge> get_g_v4_bothEX1_createdX() {
            return g.v(4).bothE(1, "created");
        }

        public Iterator<String> get_g_V_inEX2_knowsX_outV_name() {
            return g.V().inE(2, "knows").outV().value("name");
        }

        public Iterator<Vertex> get_g_v1_outE_inV() {
            return g.v(1).outE().inV();
        }

        public Iterator<Vertex> get_g_v2_inE_outV() {
            return g.v(2).inE().outV();
        }

        public Iterator<Vertex> get_g_V_outE_hasXweight_1X_outV() {
            return g.V().outE().has("weight", 1.0f).outV();
        }

        public Iterator<String> get_g_V_out_outE_inV_inE_inV_both_name() {
            return g.V().out().outE().inV().inE().inV().both().value("name");
        }

        public Iterator<String> get_g_v1_outEXknowsX_bothV_name() {
            return g.v(1).outE("knows").bothV().value("name");
        }

        public Iterator<Vertex> get_g_v1_outXknowsX() {
            return g.v(1).out("knows");
        }

        public Iterator<Vertex> get_g_v1_outXknows_createdX() {
            return g.v(1).out("knows", "created");
        }

        public Iterator<Vertex> get_g_v1_outEXknowsX_inV() {
            return g.v(1).outE("knows").inV();
        }

        public Iterator<Vertex> get_g_v1_outEXknows_createdX_inV() {
            return g.v(1).outE("knows", "created").inV();
        }

        public Iterator<Vertex> get_g_V_out_out() {
            return g.V().out().out();
        }

        public Iterator<Vertex> get_g_v1_out_out_out() {
            return g.v(1).out().out().out();
        }

        public Iterator<String> get_g_v1_out_propertyXnameX() {
            return g.v(1).out().value("name");
        }
    }
}
