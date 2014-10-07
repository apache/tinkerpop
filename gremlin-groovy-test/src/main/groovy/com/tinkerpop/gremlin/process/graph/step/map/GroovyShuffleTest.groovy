package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyShuffleTest {

    public static class StandardTest extends ShuffleTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_asXaX_outXcreatedX_asXbX_shuffle_select() {
            g.V.as('a').out('created').as('b').shuffle.select();
        }
    }

    public static class ComputerTest extends ShuffleTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_asXaX_outXcreatedX_asXbX_shuffle_select() {
            ComputerTestHelper.compute("g.V.as('a').out('created').as('b').shuffle.select()", g);
        }
    }
}
