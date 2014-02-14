package com.tinkerpop.gremlin.process.steps.filter;

import com.tinkerpop.gremlin.AbstractToyGraphGremlinTest;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ExceptTest extends AbstractToyGraphGremlinTest {

    public abstract Iterator<Vertex> get_g_v1_out_exceptXg_v2X();

    public abstract Iterator<Vertex> get_g_v1_out_aggregateXxX_out_exceptXxX();

    public abstract Iterator<String> get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX();

    @Test
    public void g_v1_out_exceptXg_v2X() {
        final Iterator<Vertex> step = get_g_v1_out_exceptXg_v2X();
        System.out.println("Testing: " + step);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<Vertex>();
        while (step.hasNext()) {
            counter++;
            Vertex vertex = step.next();
            vertices.add(vertex);
            assertTrue(vertex.getValue("name").equals("josh") || vertex.getValue("name").equals("lop"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    @Test
    public void g_v1_out_aggregateXxX_out_exceptXxX() {
        Iterator<Vertex> step = get_g_v1_out_aggregateXxX_out_exceptXxX();
        System.out.println("Testing: " + step);
        assertEquals("ripple", step.next().<String>getValue("name"));
        assertFalse(step.hasNext());
    }

    @Test
    public void g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX() {
        Iterator<String> step = get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX();
        System.out.println("Testing: " + step);
        List<String> names = Arrays.asList(step.next(), step.next());
        assertFalse(step.hasNext());
        assertEquals(2, names.size());
        assertTrue(names.contains("peter"));
        assertTrue(names.contains("josh"));
    }

    public static class JavaExceptTest extends ExceptTest {
        public Iterator<Vertex> get_g_v1_out_exceptXg_v2X() {
            return g.v(1).out().except(g.v(2));
        }

        public Iterator<Vertex> get_g_v1_out_aggregateXxX_out_exceptXxX() {
            return g.v(1).out().aggregate("x").out().except("x");
        }

        public Iterator<String> get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX() {
            return g.v(1).out("created").in("created").except(g.v(1)).value("name");
        }
    }
}
