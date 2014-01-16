package com.tinkerpop.gremlin.test.filter;

import com.tinkerpop.blueprints.Vertex;

import java.util.Iterator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IntervalTest {

    public void testCompliance() {
        assertTrue(true);
    }

    public void g_v1_outE_intervalXweight_0_06X_inV(final Iterator<Vertex> pipe) {
        while (pipe.hasNext()) {
            Vertex vertex = pipe.next();
            assertTrue(vertex.getValue("name").equals("vadas") || vertex.getValue("name").equals("lop"));
        }
        assertFalse(pipe.hasNext());
    }
}
