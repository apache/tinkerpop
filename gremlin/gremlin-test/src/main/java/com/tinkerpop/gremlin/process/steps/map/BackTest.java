package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
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
public abstract class BackTest extends AbstractGremlinTest {

    public abstract Iterator<Vertex> get_g_v1_asXhereX_out_backXhereX();

    public abstract Iterator<Vertex> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX();

    public abstract Iterator<String> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX();

    public abstract Iterator<Edge> get_g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX();

    public abstract Iterator<Edge> get_g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_asXhereX_out_backXhereX() {
        final Iterator<Vertex> step = get_g_v1_asXhereX_out_backXhereX();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            assertEquals("marko", step.next().<String>getValue("name"));
        }
        assertEquals(3, counter);
    }


    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v4_out_asXhereX_hasXlang_javaX_backXhereX() {
        final Iterator<Vertex> step = get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            final Vertex vertex = step.next();
            assertEquals("java", vertex.<String>getValue("lang"));
            assertTrue(vertex.getValue("name").equals("ripple") || vertex.getValue("name").equals("lop"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX() {
        final Iterator<Edge> step = get_g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX();
        System.out.println("Testing: " + step);
        final Edge edge = step.next();
        assertEquals("knows", edge.getLabel());
        assertEquals("7", edge.getId());
        assertEquals(0.5f, edge.<Float>getValue("weight"), 0.0001f);
        assertFalse(step.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX() {
        final Iterator<String> step = get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX();
        System.out.println("Testing: " + step);
        int counter = 0;
        final Set<String> names = new HashSet<>();
        while (step.hasNext()) {
            counter++;
            names.add(step.next());
        }
        assertEquals(2, counter);
        assertEquals(2, names.size());
        assertTrue(names.contains("ripple"));
        assertTrue(names.contains("lop"));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    @Ignore("This has to do with as labeling a filter now that its not rolled into VertexQueryStep")
    public void g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX() {
        final Iterator<Edge> step = get_g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX();
        System.out.println("Testing: " + step);
        assertTrue(step.hasNext());
        assertTrue(step.hasNext());
        Edge edge = step.next();
        assertEquals("8", edge.getId());
        assertEquals("knows", edge.getLabel());
        assertEquals(Float.valueOf(1.0f), edge.<Float>getValue("weight"));
        assertFalse(step.hasNext());
        assertFalse(step.hasNext());
    }

    public static class JavaBackTest extends BackTest {
        public Iterator<Vertex> get_g_v1_asXhereX_out_backXhereX() {
            return g.v(1).as("here").out().back("here");
        }

        public Iterator<Vertex> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX() {
            return g.v(4).out().as("here").has("lang", "java").back("here");
        }

        public Iterator<String> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX() {
            return g.v(4).out().as("here").has("lang", "java").back("here").value("name");
        }

        public Iterator<Edge> get_g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX() {
            return g.v(1).outE().as("here").inV().has("name", "vadas").back("here");
        }

        public Iterator<Edge> get_g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX() {
            return g.v(1).outE("knows").has("weight", 1.0f).as("here").inV().has("name", "josh").back("here");
        }
    }
}
