package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyHiddenValueMapTest {

    public static class StandardTest extends HiddenValueMapTest {

        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_hiddenValueMap() {
            g.V.hiddenValueMap
        }

        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_hiddenValueMapXnameX() {
            g.V.hiddenValueMap('name');
        }

    }

    public static class ComputerTest extends HiddenValueMapTest {

        public StandardTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_hiddenValueMap() {
            ComputerTestHelper.compute("g.V.hiddenValueMap", g)
        }

        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_hiddenValueMapXnameX() {
            ComputerTestHelper.compute("g.V.hiddenValueMap('name')", g)
        }

    }
}
