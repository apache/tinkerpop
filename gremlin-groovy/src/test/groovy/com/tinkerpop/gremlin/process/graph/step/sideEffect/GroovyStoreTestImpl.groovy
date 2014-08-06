package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyStoreTestImpl extends StoreTest {

    public Traversal<Vertex, Collection> get_g_V_storeXnameX_asXaX_out_capXaX() {
        return g.V.store{ it.value('name') }.as('a').out().cap('a');
    }
}
