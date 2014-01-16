package com.tinkerpop.gremlin.test.filter;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StreamFactory;
import junit.framework.TestCase;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HasTest {

    public void testCompliance() {
        assertTrue(true);
    }

    public void test_g_V_hasXname_markoX(final Iterator<Vertex> pipe) {
        assertEquals("marko", pipe.next().<String>getValue("name"));
        assertFalse(pipe.hasNext());
    }

    public void test_g_V_hasXname_blahX(final Iterator<Vertex> pipe) {
        assertFalse(pipe.hasNext());
    }

    public void test_g_V_hasXage_gt_30X(final Iterator<Vertex> pipe) {
        final List<Vertex> list = StreamFactory.stream(pipe).collect(Collectors.toList());
        assertEquals(2, list.size());
        for (final Vertex v : list) {
            assertTrue(v.<Integer>getValue("age") > 30);
        }
    }

    public void test_g_v1_out_hasXid_2X(final Iterator<Vertex> pipe) {
        assertTrue(pipe.hasNext());
        assertEquals("2", pipe.next().getId().toString());
    }

    public void test_g_V_hasXblahX(final Iterator<Vertex> pipe) {
        assertFalse(pipe.hasNext());
    }

    public void test_g_E_hasXlabelXknowsX(final Iterator<Edge> pipe) {
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            assertEquals("knows", pipe.next().getLabel());
        }
        assertEquals(2, counter);
    }

    public void test_g_E_hasXlabelXknows_createdX(final Iterator<Edge> pipe) {
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            final String label = pipe.next().getLabel();
            assertTrue(label.equals("knows") || label.equals("created"));
        }
        assertEquals(6, counter);
    }

}