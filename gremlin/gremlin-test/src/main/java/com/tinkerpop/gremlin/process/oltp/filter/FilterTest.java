package com.tinkerpop.gremlin.process.oltp.filter;

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

    public void testCompliance() {
        assertTrue(true);
    }

    public void g_V_filterXfalseX(final Iterator<Vertex> pipe) {
        System.out.println("Testing: " + pipe);
        assertFalse(pipe.hasNext());
        assertFalse(pipe.hasNext());
    }

    public void g_V_filterXtrueX(final Iterator<Vertex> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<Vertex>();
        while (pipe.hasNext()) {
            counter++;
            vertices.add(pipe.next());
        }
        assertEquals(6, counter);
        assertEquals(6, vertices.size());
        assertFalse(pipe.hasNext());
    }

    public void g_V_filterXlang_eq_javaX(final Iterator<Vertex> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<Vertex>();
        while (pipe.hasNext()) {
            counter++;
            Vertex vertex = pipe.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("ripple") ||
                    vertex.getValue("name").equals("lop"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    public void g_v1_out_filterXage_gt_30X(final Iterator<Vertex> pipe) {
        System.out.println("Testing: " + pipe);
        assertEquals(Integer.valueOf(32), pipe.next().<Integer>getValue("age"));
        assertFalse(pipe.hasNext());
    }

    public void g_V_filterXname_startsWith_m_OR_name_startsWith_pX(final Iterator<Vertex> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<Vertex>();
        while (pipe.hasNext()) {
            counter++;
            Vertex vertex = pipe.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("marko") ||
                    vertex.getValue("name").equals("peter"));
        }
        assertEquals(counter, 2);
        assertEquals(vertices.size(), 2);
    }
}
