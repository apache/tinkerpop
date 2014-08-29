package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyUnfoldTestImpl extends UnfoldTest {

    @Override
    public Traversal<Vertex, Edge> get_g_V_mapXoutEX_unfold() {
        g.V().map { it.get().outE() }.unfold()
    }

    @Override
    public Traversal<Vertex, String> get_V_values_unfold_mapXkeyX() {
        g.V().values().unfold().map { it.get().key }
    }


}
