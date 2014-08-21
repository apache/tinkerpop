package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyStoreTestImpl extends StoreTest {

    @Override
    public Traversal<Vertex, Collection> get_g_V_storeXa_nameX_out_capXaX() {
       g.V().store('a') { it.value('name') }.out().cap('a');
    }

    @Override
    public Traversal<Vertex, Collection> get_g_v1_storeXa_nameX_out_storeXa_nameX_name_capXaX(final Object v1Id) {
       g.v(v1Id).store('a'){it.get().value('name')}.out().store('a'){it.get().value('name')}.value('name').cap('a');
    }
}
