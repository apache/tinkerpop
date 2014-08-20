package com.tinkerpop.gremlin.process.graph.step.util;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC_DOUBLE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MemoryTest extends AbstractGremlinProcessTest {
    public abstract Traversal.SideEffects get_g_V_memory();

    @Test
    @LoadGraphWith(CLASSIC_DOUBLE)
    public void g_V_memory() {
        final Traversal.SideEffects sideEffects = get_g_V_memory();
        assertFalse(sideEffects.get("a").isPresent());
        assertTrue(sideEffects.get(Graph.Key.hide("g")).isPresent());
        assertFalse(sideEffects.get("g").isPresent());
        assertTrue(Graph.class.isAssignableFrom(sideEffects.get(Graph.Key.hide("g")).get().getClass()));
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
