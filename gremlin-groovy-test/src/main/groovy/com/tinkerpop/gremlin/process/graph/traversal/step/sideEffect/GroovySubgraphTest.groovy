package com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.SubgraphTest
import com.tinkerpop.gremlin.structure.Graph
import com.tinkerpop.gremlin.structure.Vertex
import com.tinkerpop.gremlin.process.graph.traversal.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySubgraphTest {

    public static class StandardTest extends SubgraphTest {

        @Override
        public Traversal<Vertex, Graph> get_g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX(
                final Object v1Id, final Graph subgraph) {
            g.V(v1Id).withSideEffect('sg') { subgraph }.outE('knows').subgraph('sg').name.cap('sg')
        }

        @Override
        public Traversal<Vertex, String> get_g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup(
                final Graph subgraph) {
            g.V.withSideEffect('sg') { subgraph }.repeat(__.bothE('created').subgraph('sg').outV).times(5).name.dedup
        }
    }
}
