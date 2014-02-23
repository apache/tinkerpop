package com.tinkerpop.gremlin.process.steps.filter;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class HasTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_markoX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_blahX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXblahX();

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_hasXid_2X();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X();

    public abstract Traversal<Edge, Edge> get_g_E_hasXlabelXknowsX();

    public abstract Traversal<Edge, Edge> get_g_E_hasXlabelXknows_createdX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_hasXname_markoX() {
        final Iterator<Vertex> traversal = get_g_V_hasXname_markoX();
        System.out.println("Testing: " + traversal);
        assertEquals("marko", traversal.next().<String>getValue("name"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_hasXname_blahX() {
        final Iterator<Vertex> traversal = get_g_V_hasXname_blahX();
        System.out.println("Testing: " + traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_hasXage_gt_30X() {
        final Iterator<Vertex> traversal = get_g_V_hasXage_gt_30X();
        System.out.println("Testing: " + traversal);
        final List<Element> list = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(2, list.size());
        for (final Element v : list) {
            assertTrue(v.<Integer>getValue("age") > 30);
        }
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_hasXid_2X() {
        final Iterator<Vertex> traversal = get_g_v1_out_hasXid_2X();
        System.out.println("Testing: " + traversal);
        assertTrue(traversal.hasNext());
        assertEquals("2", traversal.next().getId().toString());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_hasXblahX() {
        final Iterator<Vertex> traversal = get_g_V_hasXblahX();
        System.out.println("Testing: " + traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_E_hasXlabelXknowsX() {
        final Iterator<Edge> traversal = get_g_E_hasXlabelXknowsX();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals("knows", traversal.next().getLabel());
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_E_hasXlabelXknows_createdX() {
        final Iterator<Edge> traversal = get_g_E_hasXlabelXknows_createdX();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String label = traversal.next().getLabel();
            assertTrue(label.equals("knows") || label.equals("created"));
        }
        assertEquals(6, counter);
    }

    public static class JavaHasTest extends HasTest {
        public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX() {
            return g.V().has("name", "marko");
        }

        public Traversal<Vertex, Vertex> get_g_V_hasXname_blahX() {
            return g.V().has("name", "blah");
        }

        public Traversal<Vertex, Vertex> get_g_V_hasXblahX() {
            return g.V().has("blah");
        }

        public Traversal<Vertex, Vertex> get_g_v1_out_hasXid_2X() {
            return g.v(1).out().has("id", "2");
        }

        public Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X() {
            return g.V().has("age", T.gt, 30);
        }

        public Traversal<Edge, Edge> get_g_E_hasXlabelXknowsX() {
            return g.E().has("label", "knows");
        }

        public Traversal<Edge, Edge> get_g_E_hasXlabelXknows_createdX() {
            return g.E().has("label", T.in, Arrays.asList("knows", "created"));
        }
    }

}