package com.tinkerpop.gremlin.process.steps.filter;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.util.StreamFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class HasTest extends AbstractGremlinTest {

    public abstract Iterator<Element> get_g_V_hasXname_markoX();

    public abstract Iterator<Element> get_g_V_hasXname_blahX();

    public abstract Iterator<Element> get_g_V_hasXblahX();

    public abstract Iterator<Element> get_g_v1_out_hasXid_2X();

    public abstract Iterator<Element> get_g_V_hasXage_gt_30X();

    public abstract Iterator<Element> get_g_E_hasXlabelXknowsX();

    public abstract Iterator<Element> get_g_E_hasXlabelXknows_createdX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_hasXname_markoX() {
        final Iterator<Element> step = get_g_V_hasXname_markoX();
        System.out.println("Testing: " + step);
        assertEquals("marko", step.next().<String>getValue("name"));
        assertFalse(step.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_hasXname_blahX() {
        final Iterator<Element> step = get_g_V_hasXname_blahX();
        System.out.println("Testing: " + step);
        assertFalse(step.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_hasXage_gt_30X() {
        final Iterator<Element> step = get_g_V_hasXage_gt_30X();
        System.out.println("Testing: " + step);
        final List<Element> list = StreamFactory.stream(step).collect(Collectors.toList());
        assertEquals(2, list.size());
        for (final Element v : list) {
            assertTrue(v.<Integer>getValue("age") > 30);
        }
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_hasXid_2X() {
        final Iterator<Element> step = get_g_v1_out_hasXid_2X();
        System.out.println("Testing: " + step);
        assertTrue(step.hasNext());
        assertEquals("2", step.next().getId().toString());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_hasXblahX() {
        final Iterator<Element> step = get_g_V_hasXblahX();
        System.out.println("Testing: " + step);
        assertFalse(step.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_E_hasXlabelXknowsX() {
        final Iterator<Element> step = get_g_E_hasXlabelXknowsX();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            assertEquals("knows", step.next().getLabel());
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_E_hasXlabelXknows_createdX() {
        final Iterator<Element> step = get_g_E_hasXlabelXknows_createdX();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            final String label = step.next().getLabel();
            assertTrue(label.equals("knows") || label.equals("created"));
        }
        assertEquals(6, counter);
    }

    public static class JavaHasTest extends HasTest {
        public Iterator<Element> get_g_V_hasXname_markoX() {
            return g.V().has("name", "marko");
        }

        public Iterator<Element> get_g_V_hasXname_blahX() {
            return g.V().has("name", "blah");
        }

        public Iterator<Element> get_g_V_hasXblahX() {
            return g.V().has("blah");
        }

        public Iterator<Element> get_g_v1_out_hasXid_2X() {
            return g.v(1).out().has("id", "2");
        }

        public Iterator<Element> get_g_V_hasXage_gt_30X() {
            return g.V().has("age", T.gt, 30);
        }

        public Iterator<Element> get_g_E_hasXlabelXknowsX() {
            return g.E().has("label", "knows");
        }

        public Iterator<Element> get_g_E_hasXlabelXknows_createdX() {
            return g.E().has("label", T.in, Arrays.asList("knows", "created"));
        }
    }

}