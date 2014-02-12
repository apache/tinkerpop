package com.tinkerpop.gremlin.process.oltp.filter;

import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExceptTest {

    public void testCompliance() {
        assertTrue(true);
    }

    public void g_v1_out_exceptXg_v2X(Iterator<Vertex> step) {
        System.out.println("Testing: " + step);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<Vertex>();
        while (step.hasNext()) {
            counter++;
            Vertex vertex = step.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("josh") || vertex.getValue("name").equals("lop"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    public void g_v1_out_aggregateXxX_out_exceptXxX(Iterator<Vertex> step) {
        System.out.println("Testing: " + step);
        assertEquals("ripple", step.next().<String>getValue("name"));
        assertFalse(step.hasNext());
    }

    public void g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX(Iterator<String> step) {
        System.out.println("Testing: " + step);
        List<String> names = Arrays.asList(step.next(), step.next());
        assertFalse(step.hasNext());
        assertEquals(2, names.size());
        assertTrue(names.contains("peter"));
        assertTrue(names.contains("josh"));
    }
}
