package com.tinkerpop.gremlin.process.steps.filter;

import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RetainTest {

    public void g_v1_out_retainXg_v2X(Iterator<Vertex> step) {
        System.out.println("Testing: " + step);
        assertEquals("vadas", step.next().<String>getValue("name"));
        assertFalse(step.hasNext());
    }

    public void g_v1_out_aggregateXxX_out_retainXxX(Iterator<Vertex> step) {
        System.out.println("Testing: " + step);
        assertEquals("lop", step.next().<String>getValue("name"));
        assertFalse(step.hasNext());
    }
}
