package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyCoinTest {

    public static class StandardTest extends CoinTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coinX1X() {
            g.V.coin(1.0f)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coinX0X() {
            g.V.coin(0.0f)
        }
    }

    public static class ComputerTest extends CoinTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coinX1X() {
            ComputerTestHelper.compute("g.V.coin(1.0f)", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coinX0X() {
            ComputerTestHelper.compute("g.V.coin(0.0f)", g);
        }
    }
}
