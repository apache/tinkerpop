package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyRandomTest {

    public static class StandardTest extends RandomTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_randomX1X() {
            g.V.random(1.0f)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_randomX0X() {
            g.V.random(0.0f)
        }
    }

    public static class ComputerTest extends RandomTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_randomX1X() {
            ComputerTestHelper.compute("g.V.random(1.0f)",g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_randomX0X() {
            ComputerTestHelper.compute("g.V.random(0.0f)",g);
        }
    }
}
