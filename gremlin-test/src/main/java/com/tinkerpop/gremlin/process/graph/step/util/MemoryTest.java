package com.tinkerpop.gremlin.process.graph.step.util;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MemoryTest extends AbstractGremlinProcessTest {
    public abstract Traversal.SideEffects get_g_V_memory();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_memory() {
        final Traversal.SideEffects sideEffects = get_g_V_memory();
        try {
            assertFalse(sideEffects.get("a"));
        } catch (IllegalArgumentException e) {
            assertEquals(Traversal.SideEffects.Exceptions.sideEffectDoesNotExist("a").getMessage(), e.getMessage());
        }
        assertEquals(sideEffects.get(Graph.Key.hide("g")), sideEffects.getGraph());
        assertTrue(Graph.class.isAssignableFrom(sideEffects.getGraph().getClass()));
    }

    public static class JavaSideEffectsTest extends MemoryTest {
        public JavaSideEffectsTest() {
            requiresGraphComputer = false;
        }

        public Traversal.SideEffects get_g_V_memory() {
            return g.V().sideEffects();
        }
    }
}
