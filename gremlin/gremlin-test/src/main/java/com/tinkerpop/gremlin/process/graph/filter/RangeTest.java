package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Iterator;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class RangeTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_rangeX0_1X();

    public abstract Traversal<Vertex, Vertex> get_g_V_outX1X_rangeX0_2X();

    public abstract Traversal<Vertex, Vertex> get_g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV();

    public abstract Traversal<Vertex, Vertex> get_g_v1_outXknowsX_outXcreatedX_rangeX0_0X();

    public abstract Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_rangeX1_2X();

    public abstract Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_rangeX0_1X() {
        final Iterator<Vertex> step = get_g_v1_out_rangeX0_1X();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            step.next();
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_outX1X_rangeX0_2X() {
        final Iterator<Vertex> step = get_g_V_outX1X_rangeX0_2X();
        System.out.println("Testing: " + step);
        int counter = 0;
        while (step.hasNext()) {
            counter++;
            step.next();
        }
        assertEquals(3, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV() {
        final Iterator<Vertex> traversal = get_g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().getValue("name");
            assertTrue(name.equals("lop") || name.equals("ripple"));
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outXknowsX_outXcreatedX_rangeX0_0X() {
        final Iterator<Vertex> traversal = get_g_v1_outXknowsX_outXcreatedX_rangeX0_0X();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().getValue("name");
            assertTrue(name.equals("lop") || name.equals("ripple"));
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outXcreatedX_inXcreatedX_rangeX1_2X() {
        final Iterator<Vertex> traversal = get_g_v1_outXcreatedX_inXcreatedX_rangeX1_2X();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().getValue("name");
            assertTrue(name.equals("marko") || name.equals("josh") || name.equals("peter"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV() {
        final Iterator<Vertex> traversal = get_g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().getValue("name");
            assertTrue(name.equals("marko") || name.equals("josh") || name.equals("peter"));
        }
        assertEquals(2, counter);
    }

    public static class JavaRangeTest extends RangeTest {
        public Traversal<Vertex, Vertex> get_g_v1_out_rangeX0_1X() {
            return g.v(1).out().range(0, 1);
        }

        public Traversal<Vertex, Vertex> get_g_V_outX1X_rangeX0_2X() {
            return g.V().out(1).range(0, 2);
        }

        public Traversal<Vertex, Vertex> get_g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV() {
            return g.v(1).out("knows").outE("created").range(0, 0).inV();
        }

        public Traversal<Vertex, Vertex> get_g_v1_outXknowsX_outXcreatedX_rangeX0_0X() {
            return g.v(1).out("knows").out("created").range(0, 0);
        }

        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_rangeX1_2X() {
            return g.v(1).out("created").in("created").range(1, 2);
        }

        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV() {
            return g.v(1).out("created").inE("created").range(1, 2).outV();
        }
    }
}
