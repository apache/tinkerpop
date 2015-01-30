package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

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
public abstract class CoinTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_coinX1X();

    public abstract Traversal<Vertex, Vertex> get_g_V_coinX0X();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_coinX1X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_coinX1X();
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
    public void g_V_coinX0X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_coinX0X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(0, counter);
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends CoinTest {

        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coinX1X() {
            return g.V().coin(1.0d);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coinX0X() {
            return g.V().coin(0.0d);
        }
    }

    public static class ComputerTest extends CoinTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coinX1X() {
            return g.V().coin(1.0d).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coinX0X() {
            return g.V().coin(0.0d).submit(g.compute());
        }
    }
}
