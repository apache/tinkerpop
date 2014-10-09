package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class RangeTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_rangeX0_1X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_outX1X_rangeX0_2X();

    public abstract Traversal<Vertex, Vertex> get_g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_v1_outXknowsX_outXcreatedX_rangeX0_0X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_rangeX1_2X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_asXaX_both_jumpXa_3X_rangeX5_10X();

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_out_rangeX0_1X() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_out_rangeX0_1X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outX1X_rangeX0_2X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_outX1X_rangeX0_2X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(3, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("lop") || name.equals("ripple"));
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outXknowsX_outXcreatedX_rangeX0_0X() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_outXknowsX_outXcreatedX_rangeX0_0X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("lop") || name.equals("ripple"));
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outXcreatedX_inXcreatedX_rangeX1_2X() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_outXcreatedX_inXcreatedX_rangeX1_2X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("marko") || name.equals("josh") || name.equals("peter"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV() {
        final Traversal<Vertex, Vertex> traversal = get_g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("marko") || name.equals("josh") || name.equals("peter"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_asXaX_both_jumpXa_3X_rangeX5_10X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_asXaX_both_jumpXa_3X_rangeX5_10X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            traversal.next();
            counter++;
        }
        assertEquals(6, counter);
    }

    public static class StandardTest extends RangeTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_rangeX0_1X(final Object v1Id) {
            return g.v(v1Id).out().range(0, 1);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_outX1X_rangeX0_2X() {
            return g.V().out(1).range(0, 2);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXknowsX_outEXcreatedX_rangeX0_0X_inV(final Object v1Id) {
            return g.v(v1Id).out("knows").outE("created").range(0, 0).inV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXknowsX_outXcreatedX_rangeX0_0X(final Object v1Id) {
            return g.v(v1Id).out("knows").out("created").range(0, 0);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_rangeX1_2X(final Object v1Id) {
            return g.v(v1Id).out("created").in("created").range(1, 2);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inEXcreatedX_rangeX1_2X_outV(final Object v1Id) {
            return g.v(v1Id).out("created").inE("created").range(1, 2).outV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXaX_both_jumpXa_3X_rangeX5_10X() {
            return g.V().as("a").both().jump("a", 3).range(5, 10);
        }
    }
}
