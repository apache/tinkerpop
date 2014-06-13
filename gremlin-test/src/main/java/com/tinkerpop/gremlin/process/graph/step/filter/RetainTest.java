package com.tinkerpop.gremlin.process.graph.step.filter;

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

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_retainXg_v2X(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_retainXxX(final Object v1Id);

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_retainXg_v2X() {
        final Iterator<Vertex> traversal = get_g_v1_out_retainXg_v2X(convertToVertexId("marko"), convertToVertexId("vadas"));
        System.out.println("Testing: " + traversal);
        assertEquals("vadas", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_aggregateXxX_out_retainXxX() {
        final Iterator<Vertex> traversal = get_g_v1_out_aggregateXxX_out_retainXxX(convertToVertexId("marko"));
        System.out.println("Testing: " + traversal);
        assertEquals("lop", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
    }

    public static class JavaRetainTest extends RetainTest {

        public Traversal<Vertex, Vertex> get_g_v1_out_retainXg_v2X(final Object v1Id, final Object v2Id) {
            return g.v(v1Id).out().retain(g.v(v2Id));
        }

        public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_retainXxX(final Object v1Id) {
            return g.v(v1Id).out().aggregate("x").out().retain("x");
        }
    }

    public static class JavaComputerRetainTest extends RetainTest {

        public Traversal<Vertex, Vertex> get_g_v1_out_retainXg_v2X(final Object v1Id, final Object v2Id) {
            return g.v(v1Id).out().retain(g.v(v2Id)).submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_retainXxX(final Object v1Id) {
            return g.v(v1Id).out().aggregate("x").out().retain("x").submit(g.compute());
        }
    }
}
