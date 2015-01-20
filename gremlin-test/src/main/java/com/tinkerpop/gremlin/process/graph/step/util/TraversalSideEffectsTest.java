package com.tinkerpop.gremlin.process.graph.step.util;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.TraversalSideEffects;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TraversalSideEffectsTest extends AbstractGremlinProcessTest {
    public abstract TraversalSideEffects get_g_V_asAdmin_getSideEffects();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_sideEffects() {
        final TraversalSideEffects sideEffects = get_g_V_asAdmin_getSideEffects();
        try {
            assertFalse(sideEffects.get("a"));
        } catch (IllegalArgumentException e) {
            assertEquals(TraversalSideEffects.Exceptions.sideEffectDoesNotExist("a").getMessage(), e.getMessage());
        }
        assertEquals(StringFactory.traversalSideEffectsString(sideEffects), sideEffects.toString());
    }

    public static class StandardTest extends TraversalSideEffectsTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public TraversalSideEffects get_g_V_asAdmin_getSideEffects() {
            return g.V().asAdmin().getSideEffects();
        }
    }
}
