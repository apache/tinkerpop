package com.tinkerpop.gremlin.process.oltp.filter;

import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RangeTest {

    public void testCompliance() {
        assertTrue(true);
    }

    public void g_v1_out_rangeX0_1X(final Iterator<Vertex> step) {
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            step.next();
        }
        assertEquals(2, counter);
    }

    public void g_V_outX1X_rangeX0_2X(final Iterator<Vertex> step) {
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            step.next();
        }
        assertEquals(3, counter);
    }

    public void g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV(final Iterator<Vertex> step) {
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            final String name = step.next().getValue("name");
            assertTrue(name.equals("lop") || name.equals("ripple"));
        }
        assertEquals(1, counter);
    }

    public void g_v1_outXknowsX_outXcreatedX_rangeX0_0X(final Iterator<Vertex> step) {
        this.g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV(step);
    }

    public void g_v1_outXcreatedX_inXcreatedX_rangeX1_2X(final Iterator<Vertex> step) {
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            final String name = step.next().getValue("name");
            assertTrue(name.equals("marko") || name.equals("josh") || name.equals("peter"));
        }
        assertEquals(2, counter);
    }

    public void g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV(final Iterator<Vertex> step) {
        this.g_v1_outXcreatedX_inXcreatedX_rangeX1_2X(step);
    }
}
