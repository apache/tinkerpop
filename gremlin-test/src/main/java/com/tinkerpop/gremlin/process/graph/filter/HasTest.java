package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
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
import static org.junit.Assume.assumeTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class HasTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_markoX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_blahX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXblahX();

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_hasXid_2X(final Object v1Id, final Object v2Id);

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
        final Iterator<Vertex> traversal = get_g_v1_out_hasXid_2X(convertToId("marko"), convertToId("vadas"));
        System.out.println("Testing: " + traversal);
        assertTrue(traversal.hasNext());
        assertEquals(convertToId("vadas"), traversal.next().getId());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_hasXblahX() {
        assumeTrue(graphMeetsTestRequirements());
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
        public JavaHasTest() {
            requiresGraphComputer = false;
        }

        public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX() {
            return g.V().has("name", "marko");
        }

        public Traversal<Vertex, Vertex> get_g_V_hasXname_blahX() {
            return g.V().has("name", "blah");
        }

        public Traversal<Vertex, Vertex> get_g_V_hasXblahX() {
            return g.V().has("blah");
        }

        public Traversal<Vertex, Vertex> get_g_v1_out_hasXid_2X(final Object v1Id, final Object v2Id) {
            return g.v(v1Id).out().has(Element.ID, v2Id);
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

    public static class JavaComputerHasTest extends HasTest {
        public JavaComputerHasTest() {
            requiresGraphComputer = true;
        }

        public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX() {
            return g.V().<Vertex>has("name", "marko").submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_V_hasXname_blahX() {
            return g.V().<Vertex>has("name", "blah").submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_V_hasXblahX() {
            return g.V().<Vertex>has("blah").submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_v1_out_hasXid_2X(final Object v1Id, final Object v2Id) {
            return g.v(v1Id).out().<Vertex>has(Element.ID, v2Id).submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X() {
            return g.V().<Vertex>has("age", T.gt, 30).submit(g.compute());
        }

        public Traversal<Edge, Edge> get_g_E_hasXlabelXknowsX() {
            return g.E().<Edge>has("label", "knows").submit(g.compute());
        }

        public Traversal<Edge, Edge> get_g_E_hasXlabelXknows_createdX() {
            return g.E().<Edge>has("label", T.in, Arrays.asList("knows", "created")).submit(g.compute());
        }
    }

}