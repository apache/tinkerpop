package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class RangeTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out_limitX2X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_localXoutE_limitX1X_inVX_limitX3X();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_rangeX5_11X();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_limitX2X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_limitX2X(convertToVertexId("marko"));
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
    public void g_V_localXoutE_limitX1X_inVX_limitX3X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_localXoutE_limitX1X_inVX_limitX3X();
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
    public void g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(convertToVertexId("marko"));
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
    public void g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(convertToVertexId("marko"));
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
    public void g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(convertToVertexId("marko"));
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
    public void g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(convertToVertexId("marko"));
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
    public void g_V_repeatXbothX_timesX3X_rangeX5_11X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_repeatXbothX_timesX3X_rangeX5_11X();
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
        public Traversal<Vertex, Vertex> get_g_VX1X_out_limitX2X(final Object v1Id) {
            return g.V(v1Id).out().limit(2);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_localXoutE_limitX1X_inVX_limitX3X() {
            return g.V().local(__.outE().limit(1)).inV().limit(3);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(final Object v1Id) {
            return g.V(v1Id).out("knows").outE("created").range(0, 1).inV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(final Object v1Id) {
            return g.V(v1Id).out("knows").out("created").range(0, 1);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(final Object v1Id) {
            return g.V(v1Id).out("created").in("created").range(1, 3);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(final Object v1Id) {
            return g.V(v1Id).out("created").inE("created").range(1, 3).outV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_rangeX5_11X() {
            return g.V().repeat(__.both()).times(3).range(5, 11);
        }
    }
}
