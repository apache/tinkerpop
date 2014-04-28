package com.tinkerpop.gremlin.process.graph.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyOrderTest extends OrderTest {

    public Traversal<Vertex, String> get_g_V_name_order() {
        g.V.name.order
    }

    public Traversal<Vertex,String> get_g_V_name_orderXabX() {
        g.V.name.order{a,b -> b.get() <=> a.get()}
    }

    public Traversal<Vertex,String> get_g_V_orderXa_nameXb_nameX_name() {
        g.V.order{a,b -> a.get()['name'] <=> b.get()['name']}.name
    }
}
