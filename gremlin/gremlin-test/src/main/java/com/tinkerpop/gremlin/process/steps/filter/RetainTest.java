package com.tinkerpop.gremlin.process.steps.filter;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Iterator;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class RetainTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_retainXg_v2X();

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_retainXxX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_retainXg_v2X() {
        final Iterator<Vertex> traversal = get_g_v1_out_retainXg_v2X();
        System.out.println("Testing: " + traversal);
        assertEquals("vadas", traversal.next().<String>getValue("name"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_aggregateXxX_out_retainXxX() {
        final Iterator<Vertex> traversal = get_g_v1_out_aggregateXxX_out_retainXxX();
        System.out.println("Testing: " + traversal);
        assertEquals("lop", traversal.next().<String>getValue("name"));
        assertFalse(traversal.hasNext());
    }

    public static class JavaRetainTest extends RetainTest {

        public Traversal<Vertex, Vertex> get_g_v1_out_retainXg_v2X() {
            return g.v(1).out().retain(g.v(2));
        }

        public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_retainXxX() {
            return g.v(1).out().aggregate("x").out().retain("x");
        }
    }
}
