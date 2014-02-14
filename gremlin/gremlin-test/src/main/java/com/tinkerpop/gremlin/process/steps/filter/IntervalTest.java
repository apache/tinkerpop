package com.tinkerpop.gremlin.process.steps.filter;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Iterator;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class IntervalTest extends AbstractGremlinTest {

    public abstract Iterator<Vertex> get_g_v1_outE_intervalXweight_0_06X_inV();

    @Test
    @LoadGraphWith(CLASSIC)
    @Ignore("Need outE on Vertex")
    public void g_v1_outE_intervalXweight_0_06X_inV() {
        final Iterator<Vertex> step = get_g_v1_outE_intervalXweight_0_06X_inV();
        System.out.println("Testing: " + step);
        while (step.hasNext()) {
            Vertex vertex = step.next();
            assertTrue(vertex.getValue("name").equals("vadas") || vertex.getValue("name").equals("lop"));
        }
        assertFalse(step.hasNext());
    }

    public static class JavaIntervalTest extends IntervalTest {

        public Iterator<Vertex> get_g_v1_outE_intervalXweight_0_06X_inV() {
            return null; // g.v(1).outE().interval("weight", 0.0f, 0.6f).inV();
        }
    }
}
