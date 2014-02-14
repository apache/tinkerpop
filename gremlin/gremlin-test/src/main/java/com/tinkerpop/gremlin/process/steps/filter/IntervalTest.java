package com.tinkerpop.gremlin.process.steps.filter;

import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IntervalTest {

    public void g_v1_outE_intervalXweight_0_06X_inV(final Iterator<Vertex> step) {
        System.out.println("Testing: " + step);
        while (step.hasNext()) {
            Vertex vertex = step.next();
            assertTrue(vertex.getValue("name").equals("vadas") || vertex.getValue("name").equals("lop"));
        }
        assertFalse(step.hasNext());
    }
}
