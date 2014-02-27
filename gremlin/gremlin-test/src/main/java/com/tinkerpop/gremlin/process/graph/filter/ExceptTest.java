package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class ExceptTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_exceptXg_v2X();

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_exceptXxX();

    public abstract Traversal<Vertex, String> get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_exceptXg_v2X() {
        final Iterator<Vertex> traversal = get_g_v1_out_exceptXg_v2X();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<Vertex>();
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("josh") || vertex.getValue("name").equals("lop"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_aggregateXxX_out_exceptXxX() {
        Iterator<Vertex> traversal = get_g_v1_out_aggregateXxX_out_exceptXxX();
        System.out.println("Testing: " + traversal);
        assertEquals("ripple", traversal.next().<String>getValue("name"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX() {
        Iterator<String> traversal = get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX();
        System.out.println("Testing: " + traversal);
        List<String> names = Arrays.asList(traversal.next(), traversal.next());
        assertFalse(traversal.hasNext());
        assertEquals(2, names.size());
        assertTrue(names.contains("peter"));
        assertTrue(names.contains("josh"));
    }

    public static class JavaExceptTest extends ExceptTest {
        public Traversal<Vertex, Vertex> get_g_v1_out_exceptXg_v2X() {
            return g.v(1).out().except(g.v(2));
        }

        public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_exceptXxX() {
            return g.v(1).out().aggregate("x").out().except("x");
        }

        public Traversal<Vertex, String> get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX() {
            return g.v(1).out("created").in("created").except(g.v(1)).value("name");
        }
    }
}
