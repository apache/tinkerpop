package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

/**
 * @author Bob Briody (http://bobbriody.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ProfileTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, TraversalMetrics> get_g_V_out_out_profile();


    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_modern_profile() {
        /*final Traversal<Vertex, TraversalMetrics> traversal = get_g_V_out_out_profile();
        printTraversalForm(traversal);
        TraversalMetrics metrics = traversal.next();
        assertFalse(traversal.hasNext());*/

        /*StepMetrics step0 = metrics.getStepMetrics("~0");
        assertEquals(6, step0.getCount());
        assertEquals(6, step0.getTraversers());

        StepMetrics step1 = metrics.getStepMetrics("~1");
        assertEquals(6, step1.getCount());
        assertEquals(6, step1.getTraversers());

        StepMetrics step2 = metrics.getStepMetrics("~2");
        assertEquals(2, step2.getCount());
        assertEquals(2, step2.getTraversers()); */
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_out_out_grateful_profile() {
        /*final Traversal<Vertex, TraversalMetrics> traversal = get_g_V_out_out_profile();
        printTraversalForm(traversal);
        TraversalMetrics metrics = traversal.next();
        assertFalse(traversal.hasNext()); */

        /*StepMetrics step0 = metrics.getStepMetrics("~0");
        assertEquals(808, step0.getCount());
        assertEquals(808, step0.getTraversers());

        StepMetrics step1 = metrics.getStepMetrics("~1");
        assertEquals(8049, step1.getCount());
        assertEquals(8049, step1.getTraversers());

        StepMetrics step2 = metrics.getStepMetrics("~2");
        assertEquals(327370, step2.getCount());
        assertEquals(327370, step2.getTraversers());*/
    }

    public static class StandardTest extends ProfileTest {

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_out_out_profile() {
            return (Traversal) g.V().out().out().profile();
        }

    }

    public static class ComputerTest extends ProfileTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_out_out_profile() {
            return (Traversal) g.V().out().out().profile().submit(g.compute());
        }

    }
}