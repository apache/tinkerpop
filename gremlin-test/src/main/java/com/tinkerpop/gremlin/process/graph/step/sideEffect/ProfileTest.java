package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.Metrics;
import com.tinkerpop.gremlin.process.util.StandardTraversalMetrics;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

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

        double totalPercentDuration = 0;
        for (Metrics m : traversalMetrics.getMetrics()) {
            totalPercentDuration += m.getPercentDuration();
        }
        assertEquals(100, totalPercentDuration, 0.000001);
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

        double totalPercentDuration = 0;
        for (Metrics m : traversalMetrics.getMetrics()) {
            totalPercentDuration += m.getPercentDuration();
        }
        assertEquals(100, totalPercentDuration, 0.000001);
    }

    public static class StandardTest extends ProfileTest {

        @Test
        @LoadGraphWith(MODERN)
        public void testProfileTimes() {
            final Traversal<Vertex, StandardTraversalMetrics> traversal = get_g_V_sleep_sleep_profile();
            printTraversalForm(traversal);

            traversal.iterate();

            TraversalMetrics traversalMetrics = traversal.asAdmin().getSideEffects().get(TraversalMetrics.METRICS_KEY);
            traversalMetrics.toString(); // ensure no exceptions are thrown

            Metrics metrics = traversalMetrics.getMetrics(1);
            // 6 elements w/ a 10ms sleep each = 60ms with 10ms for other computation
            assertTrue("Duration should be at least the length of the sleep: " + metrics.getDuration(TimeUnit.MICROSECONDS),
                    metrics.getDuration(TimeUnit.MILLISECONDS) >= 60);
            assertTrue("Sanity check that duration is within tolerant range: " + metrics.getDuration(TimeUnit.MICROSECONDS),
                    metrics.getDuration(TimeUnit.MILLISECONDS) < 80);

            // 6 elements w/ a 5ms sleep each = 30ms with 10ms for other computation
            metrics = traversalMetrics.getMetrics(2);
            assertTrue("Duration should be at least the length of the sleep: " + metrics.getDuration(TimeUnit.MICROSECONDS),
                    metrics.getDuration(TimeUnit.MILLISECONDS) >= 30);
            assertTrue("Sanity check that duration is within tolerant range: " + metrics.getDuration(TimeUnit.MICROSECONDS),
                    metrics.getDuration(TimeUnit.MILLISECONDS) < 50);

            double totalPercentDuration = 0;
            for (Metrics m : traversalMetrics.getMetrics()) {
                totalPercentDuration += m.getPercentDuration();
            }
            assertEquals(100, totalPercentDuration, 0.000001);
        }

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile() {
            return (Traversal) g.V().out().out().profile();
        }

        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_sleep_sleep_profile() {
            return (Traversal) g.V().sideEffect(new Consumer<Traverser<Vertex>>() {
                @Override
                public void accept(final Traverser<Vertex> vertexTraverser) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).sideEffect(new Consumer<Traverser<Vertex>>() {
                @Override
                public void accept(final Traverser<Vertex> vertexTraverser) {
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).profile();
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