package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.StepMetrics;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.process.util.TraversalMetricsUtil;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;

/**
 * @author Bob Briody (http://bobbriody.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ProfileTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, TraversalMetricsUtil> get_g_V_out_out_profile();


    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_modern_profile() {
        final Traversal<Vertex, TraversalMetricsUtil> traversal = get_g_V_out_out_profile();
        printTraversalForm(traversal);

        traversal.iterate();

        TraversalMetrics metrics = traversal.sideEffects().get(TraversalMetrics.METRICS_KEY);

        StepMetrics step0 = metrics.getStepMetrics(0);
        assertEquals(6, step0.getCount());
        assertEquals(6, step0.getTraversers());

        StepMetrics step1 = metrics.getStepMetrics(1);
        assertEquals(6, step1.getCount());
        assertEquals(6, step1.getTraversers());

        StepMetrics step2 = metrics.getStepMetrics(2);
        assertEquals(2, step2.getCount());
        assertEquals(2, step2.getTraversers());
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_out_out_grateful_profile() {
        final Traversal<Vertex, TraversalMetricsUtil> traversal = get_g_V_out_out_profile();
        printTraversalForm(traversal);

        traversal.iterate();
        TraversalMetrics metrics = traversal.sideEffects().get(TraversalMetrics.METRICS_KEY);

        StepMetrics step0 = metrics.getStepMetrics(0);
        assertEquals(808, step0.getCount());

        StepMetrics step1 = metrics.getStepMetrics(1);
        assertEquals(8049, step1.getCount());

        StepMetrics step2 = metrics.getStepMetrics(2);
        assertEquals(327370, step2.getCount());
    }

    public static class StandardTest extends ProfileTest {

        @Override
        public Traversal<Vertex, TraversalMetricsUtil> get_g_V_out_out_profile() {
            return (Traversal) g.V().out().out().profile();
        }

    }

    public static class ComputerTest extends ProfileTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, TraversalMetricsUtil> get_g_V_out_out_profile() {
            return (Traversal) g.V().out().out().profile().submit(g.compute());
        }

    }
}