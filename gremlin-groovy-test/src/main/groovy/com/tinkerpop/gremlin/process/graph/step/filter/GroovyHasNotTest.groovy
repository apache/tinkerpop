package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class GroovyHasNotTest {

    public static class StandardTest extends HasNotTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasNotXprop(final Object v1Id, final String propertyKey) {
            g.V(v1Id).hasNot(propertyKey)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String propertyKey) {
            g.V.hasNot(propertyKey)
        }
    }

    public static class ComputerTest extends HasNotTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasNotXprop(final Object v1Id, final String propertyKey) {
            ComputerTestHelper.compute("g.V(${v1Id}).hasNot('${propertyKey}')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String propertyKey) {
            ComputerTestHelper.compute("g.V.hasNot('${propertyKey}')", g);
        }
    }
}
