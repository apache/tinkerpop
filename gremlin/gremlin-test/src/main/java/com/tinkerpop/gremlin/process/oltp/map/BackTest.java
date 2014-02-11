package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BackTest {

    public void testCompliance() {
        assertTrue(true);
    }

    public void g_v1_asXhereX_out_backXhereX(final Iterator<Vertex> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            assertEquals("marko", pipe.next().<String>getValue("name"));
        }
        assertEquals(3, counter);
    }


    public void g_v4_out_asXhereX_hasXlang_javaX_backXhereX(final Iterator<Vertex> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            final Vertex vertex = pipe.next();
            assertEquals("java", vertex.<String>getValue("lang"));
            assertTrue(vertex.getValue("name").equals("ripple") || vertex.getValue("name").equals("lop"));
        }
        assertEquals(2, counter);
    }

    public void g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX(final Iterator<Edge> pipe) {
        System.out.println("Testing: " + pipe);
        final Edge edge = pipe.next();
        assertEquals("knows", edge.getLabel());
        assertEquals("7", edge.getId());
        assertEquals(0.5f, edge.<Float>getValue("weight"), 0.0001f);
        assertFalse(pipe.hasNext());
    }

    public void g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX(final Iterator<String> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        final Set<String> names = new HashSet<>();
        while (pipe.hasNext()) {
            counter++;
            names.add(pipe.next());
        }
        assertEquals(2, counter);
        assertEquals(2, names.size());
        assertTrue(names.contains("ripple"));
        assertTrue(names.contains("lop"));
    }

    public void g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX(final Iterator<Edge> pipe) {
        System.out.println("Testing: " + pipe);
        assertTrue(pipe.hasNext());
        assertTrue(pipe.hasNext());
        Edge edge = pipe.next();
        assertEquals("8", edge.getId());
        assertEquals("knows", edge.getLabel());
        assertEquals(Float.valueOf(1.0f), edge.<Float>getValue("weight"));
        assertFalse(pipe.hasNext());
        assertFalse(pipe.hasNext());
    }
}
