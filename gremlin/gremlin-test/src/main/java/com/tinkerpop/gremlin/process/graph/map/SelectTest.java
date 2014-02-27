package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.graph.util.As;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Iterator;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class SelectTest extends AbstractGremlinTest {

    public abstract Iterator<Path> get_g_v1_asXaX_outXknowsX_asXbX_select();

    public abstract Iterator<Path> get_g_v1_asXaX_outXknowsX_asXbX_selectXnameX();

    public abstract Iterator<Path> get_g_v1_asXaX_outXknowsX_asXbX_selectXaX();

    public abstract Iterator<Path> get_g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_asXaX_outXknowsX_asXbX_select() {
        final Iterator<Path> step = get_g_v1_asXaX_outXknowsX_asXbX_select();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            Path path = step.next();
            assertEquals(2, path.size());
            assertEquals("1", ((Vertex) path.get(0)).getId().toString());
            assertTrue(((Vertex) path.get(1)).getId().toString().equals("2") || ((Vertex) path.get(1)).getId().toString().equals("4"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_asXaX_outXknowsX_asXbX_selectXnameX() {
        final Iterator<Path> step = get_g_v1_asXaX_outXknowsX_asXbX_selectXnameX();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            Path path = step.next();
            assertEquals(2, path.size());
            assertEquals("marko", path.get(0).toString());
            assertTrue(path.get(1).toString().equals("josh") || path.get(1).toString().equals("vadas"));
            assertTrue(path.get("b").toString().equals("josh") || path.get("b").toString().equals("vadas"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_asXaX_outXknowsX_asXbX_selectXaX() {
        final Iterator<Path> step = get_g_v1_asXaX_outXknowsX_asXbX_selectXaX();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            Path path = step.next();
            assertEquals(1, path.size());
            assertEquals("1", ((Vertex) path.get(0)).getId().toString());
            assertEquals("1", ((Vertex) path.get("a")).getId().toString());
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX() {
        final Iterator<Path> step = get_g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            Path path = step.next();
            assertEquals(1, path.size());
            assertEquals("marko", path.get(0).toString());
            assertEquals("marko", path.get("a").toString());
        }
        assertEquals(2, counter);
    }

    public static class JavaSelectTest extends SelectTest {

        public Iterator<Path> get_g_v1_asXaX_outXknowsX_asXbX_select() {
            return g.v(1).as("a").out("knows").as("b").select();
        }

        public Iterator<Path> get_g_v1_asXaX_outXknowsX_asXbX_selectXnameX() {
            return g.v(1).as("a").out("knows").as("b").select(v -> ((Vertex) v).getValue("name"));
        }

        public Iterator<Path> get_g_v1_asXaX_outXknowsX_asXbX_selectXaX() {
            return g.v(1).as("a").out("knows").as("b").select(As.of("a"));
        }

        public Iterator<Path> get_g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX() {
            return g.v(1).as("a").out("knows").as("b").select(As.of("a"), v -> ((Vertex) v).getValue("name"));
        }
    }
}
