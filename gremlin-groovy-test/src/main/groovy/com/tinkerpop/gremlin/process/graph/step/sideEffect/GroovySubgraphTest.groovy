package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Graph
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySubgraphTest {

    public static class StandardTest extends SubgraphTest {

        @Override
        public Traversal<Vertex, Graph> get_g_VX1X_outE_subgraphXknowsX_name_capXsgX(
                final Object v1Id, final Graph subgraph) {
            g.V(v1Id).withSideEffect('sg') { subgraph }.outE.subgraph('sg') { it.label() == 'knows' }.name.cap('sg')
        }

        @Override
        public Traversal<Vertex, String> get_g_V_inE_subgraphXcreatedX_name(final Graph subgraph) {
            return g.V.withSideEffect('sg') { subgraph }.inE.subgraph('sg') { it.label() == "created" }.name;
        }
    }
}
