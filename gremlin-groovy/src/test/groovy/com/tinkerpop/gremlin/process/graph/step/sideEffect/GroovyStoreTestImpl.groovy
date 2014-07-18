package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyStoreTestImpl extends StoreTest {

    public Traversal<Vertex, Collection> get_g_V_storeXa_nameX_out_capXaX() {
        return g.V.store('a') { it.value('name') }.out().cap('a');
    }
}
