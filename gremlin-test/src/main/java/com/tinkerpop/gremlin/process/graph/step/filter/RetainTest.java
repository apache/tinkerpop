package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class RetainTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_retainXg_v2X(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_retainXxX(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_out_retainXg_v2X() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_out_retainXg_v2X(convertToVertexId("marko"), convertToVertexId("vadas"));
        printTraversalForm(traversal);
        assertEquals("vadas", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_out_aggregateXxX_out_retainXxX() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_out_aggregateXxX_out_retainXxX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals("lop", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends RetainTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_retainXg_v2X(final Object v1Id, final Object v2Id) {
            return g.v(v1Id).out().retain(g.v(v2Id));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_retainXxX(final Object v1Id) {
            return g.v(v1Id).out().aggregate("x").out().retain("x");
        }
    }

    public static class ComputerTest extends RetainTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_retainXg_v2X(final Object v1Id, final Object v2Id) {
            return g.v(v1Id).out().retain(g.v(v2Id)).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_retainXxX(final Object v1Id) {
            return g.v(v1Id).out().aggregate("x").out().retain("x").submit(g.compute());
        }
    }
}
