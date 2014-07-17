package com.tinkerpop.gremlin.process.graph.step.util;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MemoryTest extends AbstractGremlinProcessTest {
    public abstract Traversal.Memory get_g_V_memory();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_memory() {
        final Traversal.Memory memory = get_g_V_memory();
        try {
            memory.get("a");
            fail("Accessing a memory element that does not exist should not return null");
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }

    public static class JavaMemoryTest extends MemoryTest {
        public JavaMemoryTest() {
            requiresGraphComputer = false;
        }

        public Traversal.Memory get_g_V_memory() {
            return g.V().memory();
        }
    }
}
