package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class FilterTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXfalseX();

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXtrueX();

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX();

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_filterXage_gt_30X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_filterXfalseX() {
        assumeTrue(graphMeetsTestRequirements());
        final Iterator<Vertex> traversal = get_g_V_filterXfalseX();
        System.out.println("Testing: " + traversal);
        assertFalse(traversal.hasNext());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_filterXtrueX() {
        assumeTrue(graphMeetsTestRequirements());
        final Iterator<Vertex> traversal = get_g_V_filterXtrueX();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            vertices.add(traversal.next());
        }
        assertEquals(6, counter);
        assertEquals(6, vertices.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_filterXlang_eq_javaX() {
        assumeTrue(graphMeetsTestRequirements());
        final Iterator<Vertex> traversal = get_g_V_filterXlang_eq_javaX();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("ripple") ||
                    vertex.getValue("name").equals("lop"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_filterXage_gt_30X() {
        assumeTrue(graphMeetsTestRequirements());
        final Iterator<Vertex> traversal = get_g_v1_out_filterXage_gt_30X(convertToId("marko"));
        System.out.println("Testing: " + traversal);
        assertEquals(Integer.valueOf(32), traversal.next().<Integer>getValue("age"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
        assumeTrue(graphMeetsTestRequirements());
        final Iterator<Vertex> traversal = get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("marko") ||
                    vertex.getValue("name").equals("peter"));
        }
        assertEquals(counter, 2);
        assertEquals(vertices.size(), 2);
    }

    public static class JavaFilterTest extends FilterTest {
        public JavaFilterTest() {
            this.requiresGraphComputer = false;
        }

        public Traversal<Vertex, Vertex> get_g_V_filterXfalseX() {
            return g.V().filter(v -> false);
        }

        public Traversal<Vertex, Vertex> get_g_V_filterXtrueX() {
            return g.V().filter(v -> true);
        }

        public Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX() {
            return g.V().filter(v -> v.get().<String>getProperty("lang").orElse("none").equals("java"));
        }

        public Traversal<Vertex, Vertex> get_g_v1_out_filterXage_gt_30X(final Object v1Id) {
            return g.v(v1Id).out().filter(v -> v.get().<Integer>getProperty("age").orElse(0) > 30);
        }

        public Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
            return g.V().filter(v -> {
                final String name = v.get().getValue("name");
                return name.startsWith("m") || name.startsWith("p");
            });
        }
    }

    public static class JavaComputerFilterTest extends FilterTest {

        public JavaComputerFilterTest() {
            this.requiresGraphComputer = true;
        }

        public Traversal<Vertex, Vertex> get_g_V_filterXfalseX() {
            return g.V().filter(v -> false).submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_V_filterXtrueX() {
            return g.V().filter(v -> true).submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX() {
            return g.V().filter(v -> v.get().<String>getProperty("lang").orElse("none").equals("java")).submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_v1_out_filterXage_gt_30X(final Object v1Id) {
            // TODO: FIX return g.v(1).out().filter(v -> v.get().<Integer>getProperty("age").orElse(0) > 30).submit(g.compute());
            return g.V().has(Element.ID, v1Id).out().filter(v -> v.get().<Integer>getProperty("age").orElse(0) > 30).submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
            return g.V().filter(v -> {
                final String name = v.get().getValue("name");
                return name.startsWith("m") || name.startsWith("p");
            }).submit(g.compute());
        }
    }
}
