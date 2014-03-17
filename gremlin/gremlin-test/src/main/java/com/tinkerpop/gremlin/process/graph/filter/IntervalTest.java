package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Iterator;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class IntervalTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex,Vertex> get_g_v1_outE_intervalXweight_0_06X_inV(final Object v1Id);

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outE_intervalXweight_0_06X_inV() {
        final Iterator<Vertex> traversal = get_g_v1_outE_intervalXweight_0_06X_inV(convertToId("marko"));
        System.out.println("Testing: " + traversal);
        while (traversal.hasNext()) {
            Vertex vertex = traversal.next();
            assertTrue(vertex.getValue("name").equals("vadas") || vertex.getValue("name").equals("lop"));
        }
        assertFalse(traversal.hasNext());
    }

    public static class JavaIntervalTest extends IntervalTest {
        public JavaIntervalTest() {
            requiresGraphComputer = false;
        }

        public Traversal<Vertex,Vertex> get_g_v1_outE_intervalXweight_0_06X_inV(final Object v1Id) {
            return g.v(v1Id).outE().interval("weight", 0.0f, 0.6f).inV();
        }
    }

    public static class JavaComputerIntervalTest extends IntervalTest {
        public JavaComputerIntervalTest() {
            requiresGraphComputer = true;
        }

        public Traversal<Vertex,Vertex> get_g_v1_outE_intervalXweight_0_06X_inV(final Object v1Id) {
            return g.v(v1Id).outE().interval("weight", 0.0f, 0.6f).inV().submit(g.compute());
        }
    }
}
