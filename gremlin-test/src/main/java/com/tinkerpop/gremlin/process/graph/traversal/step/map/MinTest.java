package com.tinkerpop.gremlin.process.graph.traversal.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.__.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MinTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Integer> get_g_V_age_min();

    public abstract Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_min();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_age_min() {
        final List<Traversal<Vertex, Integer>> traversals = Arrays.asList(
                get_g_V_age_min(),
                get_g_V_repeatXbothX_timesX5X_age_min());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            checkResults(Arrays.asList(27), traversal);
        });
    }

    public static class StandardTest extends MinTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_min() {
            return g.V().values("age").min();
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_min() {
            return g.V().repeat(both()).times(5).values("age").min();
        }

    }

    public static class ComputerTest extends MinTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_min() {
            return g.V().values("age").<Integer>min().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_min() {
            return g.V().repeat(both()).times(5).values("age").<Integer>min().submit(g.compute());
        }
    }
}