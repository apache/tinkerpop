package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class PathTest extends AbstractGremlinTest {

    public abstract Iterator<Path> get_g_v1_propertyXnameX_path();

    public abstract Iterator<Path> get_g_v1_out_pathXage_nameX();

    public abstract Iterator<Path> get_g_V_asXxX_out_loopXx_loops_lt_3X_pathXit__name__langX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_propertyXnameX_path() {
        final Iterator<Path> step = get_g_v1_propertyXnameX_path();
        System.out.println("Testing: " + step);
        final Path path = step.next();
        assertFalse(step.hasNext());
        assertEquals(2, path.size());
        assertEquals("1", ((Vertex) path.get(0)).<String>getId());
        assertEquals("marko", ((Vertex) path.get(0)).<String>getValue("name"));
        assertEquals("marko", path.<String>get(1));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_pathXage_nameX() {
        final Iterator<Path> step = get_g_v1_out_pathXage_nameX();
        System.out.println("Testing: " + step);
        int counter = 0;
        final Set<String> names = new HashSet<>();
        while (step.hasNext()) {
            counter++;
            final Path path = step.next();
            assertEquals(Integer.valueOf(29), path.<Integer>get(0));
            assertTrue(path.get(1).equals("josh") || path.get(1).equals("vadas") || path.get(1).equals("lop"));
            names.add(path.get(1));
        }
        assertEquals(3, counter);
        assertEquals(3, names.size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_asXxX_out_loopXx_loops_lt_3X_pathXit__name__langX() {
        final Iterator<Path> step = get_g_V_asXxX_out_loopXx_loops_lt_3X_pathXit__name__langX();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            final Path path = step.next();
            assertEquals(3, path.size());
            assertEquals("marko", ((Vertex) path.get(0)).<String>getValue("name"));
            assertEquals("josh", path.<String>get(1));
            assertEquals("java", path.<String>get(2));
        }
        assertEquals(2, counter);
    }

    public static class JavaPathTest extends PathTest {
        public Iterator<Path> get_g_v1_propertyXnameX_path() {
            return g.v(1).value("name").path();
        }

        public Iterator<Path> get_g_v1_out_pathXage_nameX() {
            return g.v(1).out().path(v -> ((Vertex) v).getValue("age"), v -> ((Vertex) v).getValue("name"));
        }

        public Iterator<Path> get_g_V_asXxX_out_loopXx_loops_lt_3X_pathXit__name__langX() {
            return g.V().as("x").out()
                    .jump("x", o -> o.getLoops() < 2)
                    .path(v -> v, v -> ((Vertex) v).getValue("name"), v -> ((Vertex) v).getValue("lang"));
        }
    }
}