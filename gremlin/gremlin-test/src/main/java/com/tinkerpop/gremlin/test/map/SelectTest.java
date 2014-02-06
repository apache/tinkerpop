package com.tinkerpop.gremlin.test.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectTest {

    public void testCompliance() {
        assertTrue(true);
    }

    public void g_v1_asXaX_outXknowsX_asXbX_select(final Iterator<Path> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            Path path = pipe.next();
            assertEquals(2, path.size());
            assertEquals("1", ((Vertex) path.get(0)).getId().toString());
            assertTrue(((Vertex) path.get(1)).getId().toString().equals("2") || ((Vertex) path.get(1)).getId().toString().equals("4"));
        }
        assertEquals(2, counter);
    }

    public void g_v1_asXaX_outXknowsX_asXbX_selectXnameX(final Iterator<Path> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            Path path = pipe.next();
            assertEquals(2, path.size());
            assertEquals("marko", path.get(0).toString());
            assertTrue(path.get(1).toString().equals("josh") || path.get(1).toString().equals("vadas"));
            assertTrue(path.get("b").toString().equals("josh") || path.get("b").toString().equals("vadas"));
        }
        assertEquals(2, counter);
    }

    public void g_v1_asXaX_outXknowsX_asXbX_selectXaX(final Iterator<Path> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            Path path = pipe.next();
            assertEquals(1, path.size());
            assertEquals("1", ((Vertex) path.get(0)).getId().toString());
            assertEquals("1", ((Vertex) path.get("a")).getId().toString());
        }
        assertEquals(2, counter);
    }

    public void g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX(final Iterator<Path> pipe) {
        System.out.println("Testing: " + pipe);
        int counter = 0;
        while (pipe.hasNext()) {
            counter++;
            Path path = pipe.next();
            assertEquals(1, path.size());
            assertEquals("marko", path.get(0).toString());
            assertEquals("marko", path.get("a").toString());
        }
        assertEquals(2, counter);
    }
}
