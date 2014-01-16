package com.tinkerpop.gremlin.test.filter;

import com.tinkerpop.blueprints.Vertex;

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

    public void test_g_v1_out_rangeX0_1X(final Iterator<Vertex> pipe) {
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            pipe.next();
        }
        assertEquals(2, counter);
    }

    public void test_g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV(final Iterator<Vertex> pipe) {
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            final String name = pipe.next().getValue("name");
            assertTrue(name.equals("lop") || name.equals("ripple"));
        }
        assertEquals(1, counter);
    }

    public void test_g_v1_outXknowsX_outXcreatedX_rangeX0_0X(final Iterator<Vertex> pipe) {
        this.test_g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV(pipe);
    }

    public void test_g_v1_outXcreatedX_inXcreatedX_rangeX1_2X(final Iterator<Vertex> pipe) {
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            final String name = pipe.next().getValue("name");
            assertTrue(name.equals("marko") || name.equals("josh") || name.equals("peter"));
        }
        assertEquals(2, counter);
    }

    public void test_g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV(final Iterator<Vertex> pipe) {
        this.test_g_v1_outXcreatedX_inXcreatedX_rangeX1_2X(pipe);
    }
}
