package com.tinkerpop.gremlin.process.steps.filter;

import com.tinkerpop.gremlin.AbstractToyGraphGremlinTest;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class FilterTest extends AbstractToyGraphGremlinTest {

    public abstract Iterator<Vertex> get_g_V_filterXfalseX();

    public abstract Iterator<Vertex> get_g_V_filterXtrueX();

    public abstract Iterator<Vertex> get_g_V_filterXlang_eq_javaX();

    public abstract Iterator<Vertex> get_g_v1_out_filterXage_gt_30X();

    public abstract Iterator<Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX();

    @Test
    public void g_V_filterXfalseX() {
        final Iterator<Vertex> step = get_g_V_filterXfalseX();
        System.out.println("Testing: " + step);
        assertFalse(step.hasNext());
        assertFalse(step.hasNext());
    }

    @Test
    public void g_V_filterXtrueX() {
        final Iterator<Vertex> step = get_g_V_filterXtrueX();
        System.out.println("Testing: " + step);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<Vertex>();
        while (step.hasNext()) {
            counter++;
            vertices.add(step.next());
        }
        assertEquals(6, counter);
        assertEquals(6, vertices.size());
        assertFalse(step.hasNext());
    }

    @Test
    public void g_V_filterXlang_eq_javaX() {
        final Iterator<Vertex> step = get_g_V_filterXlang_eq_javaX();
        System.out.println("Testing: " + step);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<Vertex>();
        while (step.hasNext()) {
            counter++;
            Vertex vertex = step.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("ripple") ||
                    vertex.getValue("name").equals("lop"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    @Test
    public void g_v1_out_filterXage_gt_30X() {
        final Iterator<Vertex> step = get_g_v1_out_filterXage_gt_30X();
        System.out.println("Testing: " + step);
        assertEquals(Integer.valueOf(32), step.next().<Integer>getValue("age"));
        assertFalse(step.hasNext());
    }

    @Test
    public void g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
        final Iterator<Vertex> step = get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX();
        System.out.println("Testing: " + step);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<Vertex>();
        while (step.hasNext()) {
            counter++;
            Vertex vertex = step.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("marko") ||
                    vertex.getValue("name").equals("peter"));
        }
        assertEquals(counter, 2);
        assertEquals(vertices.size(), 2);
    }

    public static class JavaFilterTest extends FilterTest {
        public Iterator<Vertex> get_g_V_filterXfalseX() {
            return g.V().filter(v -> false);
        }

        public Iterator<Vertex> get_g_V_filterXtrueX() {
            return g.V().filter(v -> true);
        }

        public Iterator<Vertex> get_g_V_filterXlang_eq_javaX() {
            return g.V().filter(v -> v.get().<String>getProperty("lang").orElse("none").equals("java"));
        }

        public Iterator<Vertex> get_g_v1_out_filterXage_gt_30X() {
            return g.v(1).out().filter(v -> v.get().<Integer>getProperty("age").orElse(0) > 30);
        }

        public Iterator<Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
            return g.V().filter(v -> {
                final String name = v.get().getValue("name");
                return name.startsWith("m") || name.startsWith("p");
            });
        }
    }
}
