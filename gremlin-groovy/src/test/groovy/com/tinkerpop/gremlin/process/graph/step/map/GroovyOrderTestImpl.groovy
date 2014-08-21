package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyOrderTestImpl extends OrderTest {

    @Override
    public Traversal<Vertex, String> get_g_V_name_order() {
        g.V().value('name').order()
    }

    @Override
    public Traversal<Vertex, String> get_g_V_name_orderXabX() {
        g.V().value('name').order { a, b -> b.get() <=> a.get() }
    }

    @Override
    public Traversal<Vertex, String> get_g_V_orderXa_nameXb_nameX_name() {
        g.V().order { a, b -> a.get().value('name') <=> b.get().value('name') }.value('name')
    }
}
