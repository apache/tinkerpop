package com.tinkerpop.gremlin.process.oltp.filter;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.util.StreamFactory;

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

    public void g_V_hasXname_markoX(final Iterator<Element> pipe) {
        System.out.println("Testing: " + pipe);
        assertEquals("marko", pipe.next().<String>getValue("name"));
        assertFalse(pipe.hasNext());
    }

    public void g_V_hasXname_blahX(final Iterator<Element> pipe) {
        System.out.println("Testing: " + pipe);
        assertFalse(pipe.hasNext());
    }

    public void g_V_hasXage_gt_30X(final Iterator<Element> pipe) {
        System.out.println("Testing: " + pipe);
        final List<Element> list = StreamFactory.stream(pipe).collect(Collectors.toList());
        assertEquals(2, list.size());
        for (final Element v : list) {
            assertTrue(v.<Integer>getValue("age") > 30);
        }
    }

    public void g_v1_out_hasXid_2X(final Iterator<Element> pipe) {
        System.out.println("Testing: " + pipe);
        assertTrue(pipe.hasNext());
        assertEquals("2", pipe.next().getId().toString());
    }

    public void g_V_hasXblahX(final Iterator<Element> pipe) {
        System.out.println("Testing: " + pipe);
        assertFalse(pipe.hasNext());
    }

    public void g_E_hasXlabelXknowsX(final Iterator<Element> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            assertEquals("knows", pipe.next().getLabel());
        }
        assertEquals(2, counter);
    }

    public void g_E_hasXlabelXknows_createdX(final Iterator<Element> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            final String label = pipe.next().getLabel();
            assertTrue(label.equals("knows") || label.equals("created"));
        }
        assertEquals(6, counter);
    }

}