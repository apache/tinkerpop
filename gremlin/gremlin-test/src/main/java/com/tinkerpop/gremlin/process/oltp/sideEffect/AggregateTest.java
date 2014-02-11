package com.tinkerpop.gremlin.process.oltp.sideEffect;

import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AggregateTest {

    public void testCompliance() {
        assertTrue(true);
    }

    public void g_v1_aggregateXaX_outXcreatedX_inXcreatedX_exceptXaX(final Iterator<Vertex> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            Vertex vertex = pipe.next();
            assertTrue(vertex.getValue("name").equals("peter") || vertex.getValue("name").equals("josh"));
        }
        assertEquals(2, counter);
    }

    public void g_V_valueXnameX_aggregateXaX_iterate_getXaX(final List<String> names) {
        assertEquals(6, names.size());
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("peter"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("vadas"));
        assertTrue(names.contains("ripple"));
    }

    public void g_V_aggregateXa_nameX_iterate_getXaX(final List<String> names) {
        this.g_V_valueXnameX_aggregateXaX_iterate_getXaX(names);
    }
}
