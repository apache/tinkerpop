package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Path;
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
public class PathTest {

    public void testCompliance() {
        assertTrue(true);
    }

    public void g_v1_propertyXnameX_path(final Iterator<Path> pipe) {
        System.out.println("Testing: " + pipe);
        final Path path = pipe.next();
        assertFalse(pipe.hasNext());
        assertEquals(2, path.size());
        assertEquals("1", ((Vertex) path.get(0)).<String>getId());
        assertEquals("marko", ((Vertex) path.get(0)).<String>getValue("name"));
        assertEquals("marko", path.<String>get(1));
    }

    public void g_v1_out_pathXage_nameX(final Iterator<Path> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        final Set<String> names = new HashSet<>();
        while (pipe.hasNext()) {
            counter++;
            final Path path = pipe.next();
            assertEquals(Integer.valueOf(29), path.<Integer>get(0));
            assertTrue(path.get(1).equals("josh") || path.get(1).equals("vadas") || path.get(1).equals("lop"));
            names.add(path.get(1));
        }
        assertEquals(3, counter);
        assertEquals(3, names.size());
    }

    public void g_V_asXxX_out_loopXx_loops_lt_3X_pathXit__name__langX(final Iterator<Path> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            final Path path = pipe.next();
            assertEquals(3, path.size());
            assertEquals("marko", ((Vertex) path.get(0)).<String>getValue("name"));
            assertEquals("josh", path.<String>get(1));
            assertEquals("java", path.<String>get(2));
        }
        assertEquals(2, counter);
    }
}