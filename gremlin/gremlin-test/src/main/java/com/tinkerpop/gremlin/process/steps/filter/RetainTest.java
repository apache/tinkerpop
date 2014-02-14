package com.tinkerpop.gremlin.process.steps.filter;

import com.tinkerpop.gremlin.AbstractToyGraphGremlinTest;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class RetainTest extends AbstractToyGraphGremlinTest {

    public abstract Iterator<Vertex> get_g_v1_out_retainXg_v2X();

    public abstract Iterator<Vertex> get_g_v1_out_aggregateXxX_out_retainXxX();

    @Test
    public void g_v1_out_retainXg_v2X() {
        final Iterator<Vertex> step = get_g_v1_out_retainXg_v2X();
        System.out.println("Testing: " + step);
        assertEquals("vadas", step.next().<String>getValue("name"));
        assertFalse(step.hasNext());
    }

    @Test
    public void g_v1_out_aggregateXxX_out_retainXxX() {
        final Iterator<Vertex> step = get_g_v1_out_aggregateXxX_out_retainXxX();
        System.out.println("Testing: " + step);
        assertEquals("lop", step.next().<String>getValue("name"));
        assertFalse(step.hasNext());
    }

    public static class JavaRetainTest extends RetainTest {

        public Iterator<Vertex> get_g_v1_out_retainXg_v2X() {
            return g.v(1).out().retain(g.v(2));
        }

        public Iterator<Vertex> get_g_v1_out_aggregateXxX_out_retainXxX() {
            return g.v(1).out().aggregate("x").out().retain("x");
        }
    }
}
