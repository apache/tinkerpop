package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyAddEdgeTest {

    public static class StandardTest extends AddEdgeTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_addBothEXcocreator_aX(
                final Object v1Id) {
            g.V(v1Id).as('a').out('created').in('created').addBothE('cocreator', 'a')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX(final Object v1Id) {
            g.V(v1Id).as('a').out('created').addOutE('createdBy', 'a')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X(
                final Object v1Id) {
            g.V(v1Id).as('a').out('created').addOutE('createdBy', 'a', 'weight', 2)
        }
    }
}
