package com.tinkerpop.gremlin.test.map;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Path;
import junit.framework.TestCase;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathTest extends TestCase {

    public void testCompliance() {
        assertTrue(true);
    }

    public void test_g_v1_propertyXnameX_path(final Iterator<Path> pipe) {
        Path path = pipe.next();
        assertFalse(pipe.hasNext());
        assertEquals(2, path.size());
        assertEquals("marko", ((Vertex) path.get(0)).getValue("name"));
        assertEquals("marko", path.get(1));
    }

    public void test_g_v1_out_pathXage__nameX(final Iterator<Path> pipe) {
        int counter = 0;
        Set<String> names = new HashSet<String>();
        while (pipe.hasNext()) {
            counter++;
            Path path = pipe.next();
            assertEquals(Integer.valueOf(29), path.get(0));
            assertTrue(path.get(1).equals("josh") || path.get(1).equals("vadas") || path.get(1).equals("lop"));
            names.add(path.get(1));
        }
        assertEquals(3, counter);
        assertEquals(3, names.size());
    }

    public void test_g_V_out_loopX1__loops_lt_3X_pathXit__name__langX(final Iterator<Path> pipe) {
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            Path path = pipe.next();
            assertEquals(3, path.size());
            assertEquals("marko", ((Vertex) path.get(0)).getValue("name"));
            assertEquals("josh", path.get(1));
            assertEquals("java", path.get(2));
        }
        assertEquals(2, counter);
    }
}