package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.Metrics;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.process.util.StandardTraversalMetrics;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author Bob Briody (http://bobbriody.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ProfileTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile();


    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_modern_profile() {
        final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_out_out_profile();
        printTraversalForm(traversal);

        traversal.iterate();

        TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(TraversalMetrics.METRICS_KEY);
        traversalMetrics.toString(); // ensure no exceptions are thrown


        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(6, metrics.getNested(TraversalMetrics.ELEMENT_COUNT_ID).getCount());
        assertNotEquals(0, metrics.getCount());
        assertNotEquals(0, metrics.getPercentDuration());

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(6, metrics.getNested(TraversalMetrics.ELEMENT_COUNT_ID).getCount());
        assertNotEquals(0, metrics.getCount());
        assertNotEquals(0, metrics.getPercentDuration());

        metrics = traversalMetrics.getMetrics(2);
        assertEquals(2, metrics.getNested(TraversalMetrics.ELEMENT_COUNT_ID).getCount());
        assertNotEquals(0, metrics.getCount());
        assertNotEquals(0, metrics.getPercentDuration());
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_out_out_grateful_profile() {
        final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_out_out_profile();
        printTraversalForm(traversal);

        traversal.iterate();
        TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(TraversalMetrics.METRICS_KEY);
        traversalMetrics.toString(); // ensure no exceptions are thrown

        Metrics metrics = traversalMetrics.getMetrics(0);
        assertEquals(808, metrics.getNested(TraversalMetrics.ELEMENT_COUNT_ID).getCount());
        assertNotEquals(0, metrics.getCount());
        assertNotEquals(0, metrics.getPercentDuration());

        metrics = traversalMetrics.getMetrics(1);
        assertEquals(8049, metrics.getNested(TraversalMetrics.ELEMENT_COUNT_ID).getCount());
        assertNotEquals(0, metrics.getCount());
        assertNotEquals(0, metrics.getPercentDuration());

        metrics = traversalMetrics.getMetrics(2);
        assertEquals(327370, metrics.getNested(TraversalMetrics.ELEMENT_COUNT_ID).getCount());
        assertNotEquals(0, metrics.getCount());
        assertNotEquals(0, metrics.getPercentDuration());
    }

    public static class StandardTest extends ProfileTest {

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile() {
            return (Traversal) g.V().out().out().profile();
        }

    }

    public static class ComputerTest extends ProfileTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile() {
            return (Traversal) g.V().out().out().profile().submit(g.compute());
        }

    }
}