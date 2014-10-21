package com.tinkerpop.gremlin.process.graph.step.util;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TraversalStrategiesTest extends AbstractGremlinProcessTest {
    public abstract Traversal.Strategies get_g_V_strategies();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_strategies() {
        final Traversal.Strategies strategies = get_g_V_strategies();
        assertEquals(StringFactory.traversalStrategiesString(strategies), strategies.toString());
    }

    public static class StandardTest extends TraversalStrategiesTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal.Strategies get_g_V_strategies() {
            return g.V().strategies();
        }
    }
}
