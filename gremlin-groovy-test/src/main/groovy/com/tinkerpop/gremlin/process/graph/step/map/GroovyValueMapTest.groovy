package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyValueMapTest {

    public static class StandardTest extends ValueMapTest {
        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_valueMap() {
            g.V.valueMap
        }

        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_valueMapXname_ageX() {
            g.V.valueMap('name', 'age')
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_VX1X_outXcreatedX_valueMap(final Object v1Id) {
            g.V(v1Id).out('created').valueMap
        }
    }

    public static class ComputerTest extends ValueMapTest {
        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_valueMap() {
            ComputerTestHelper.compute("g.V.valueMap", g);
        }

        @Override
        public Traversal<Vertex, Map<String, List>> get_g_V_valueMapXname_ageX() {
            ComputerTestHelper.compute("g.V.valueMap('name', 'age')", g);
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_VX1X_outXcreatedX_valueMap(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out('created').valueMap", g);
        }
    }
}
