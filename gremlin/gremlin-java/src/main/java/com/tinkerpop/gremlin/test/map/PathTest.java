package com.tinkerpop.gremlin.test.map;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.pipes.Pipeline;
import com.tinkerpop.gremlin.pipes.util.Path;
import junit.framework.TestCase;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathTest extends TestCase {

    public void testCompliance() {
        TestCase.assertTrue(true);
    }

    public void test_g_v1_propertyXnameX_path(final Pipeline<Vertex, Path> pipe) {
        Path path = pipe.next().get();
        assertFalse(pipe.hasNext());
        assertEquals(path.size(), 2);
        TestCase.assertEquals(((Vertex) path.get(0)).getValue("name"), "marko");
        assertEquals(path.get(1), "marko");
    }

    public void test_g_v1_out_pathXage__nameX(final Pipeline<Vertex, Path> pipe) {
        int counter = 0;
        Set<String> names = new HashSet<String>();
        while (pipe.hasNext()) {
            counter++;
            Path path = pipe.next().get();
            assertEquals(path.get(0), Integer.valueOf(29));
            assertTrue(path.get(1).equals("josh") || path.get(1).equals("vadas") || path.get(1).equals("lop"));
            names.add(path.get(1));
        }
        TestCase.assertEquals(counter, 3);
        TestCase.assertEquals(names.size(), 3);
    }

    public void test_g_V_out_loopX1__loops_lt_3X_pathXit__name__langX(final Pipeline<Vertex, Path> pipe) {
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            Path path = pipe.next().get();
            assertEquals(path.size(), 3);
            assertEquals(((Vertex) path.get(0)).getValue("name"), "marko");
            assertEquals(path.get(1), "josh");
            assertEquals(path.get(2), "java");
        }
        TestCase.assertEquals(counter, 2);
    }
}