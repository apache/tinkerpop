package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class RandomTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_randomX1X();

    public abstract Traversal<Vertex, Vertex> get_g_V_randomX0X();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_randomX1X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_randomX1X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(6, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_randomX0X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_randomX0X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(0, counter);
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends RandomTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_randomX1X() {
            return g.V().random(1.0d);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_randomX0X() {
            return g.V().random(0.0d);
        }
    }

    public static class ComputerTest extends RandomTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_randomX1X() {
            return g.V().random(1.0d).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_randomX0X() {
            return g.V().random(0.0d).submit(g.compute());
        }
    }
}
