package com.tinkerpop.gremlin.process.steps.filter;

import com.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FilterTest {

    public void g_V_filterXfalseX(final Iterator<Vertex> step) {
        System.out.println("Testing: " + step);
        assertFalse(step.hasNext());
        assertFalse(step.hasNext());
    }

    public void g_V_filterXtrueX(final Iterator<Vertex> step) {
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

    public void g_V_filterXlang_eq_javaX(final Iterator<Vertex> step) {
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

    public void g_v1_out_filterXage_gt_30X(final Iterator<Vertex> step) {
        System.out.println("Testing: " + step);
        assertEquals(Integer.valueOf(32), step.next().<Integer>getValue("age"));
        assertFalse(step.hasNext());
    }

    public void g_V_filterXname_startsWith_m_OR_name_startsWith_pX(final Iterator<Vertex> step) {
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
}
