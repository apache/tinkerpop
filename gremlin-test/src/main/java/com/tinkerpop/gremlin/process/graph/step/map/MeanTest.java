package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MeanTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Double> get_g_V_age_mean();


    @Test
    @LoadGraphWith(MODERN)
    public void g_V_age_mean() {
        final List<Traversal<Vertex, Double>> traversals = Arrays.asList(get_g_V_age_mean());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Double mean = traversal.next();
            assertEquals(30.75, mean, 0.05);
            assertFalse(traversal.hasNext());

        });
    }

    public static class StandardTest extends MeanTest {

        @Override
        public Traversal<Vertex, Double> get_g_V_age_mean() {
            return g.V().values("age").mean();
        }

    }

    public static class ComputerTest extends MeanTest {

        @Override
        public Traversal<Vertex, Double> get_g_V_age_mean() {
            return g.V().values("age").mean().submit(g.compute());
        }

    }
}