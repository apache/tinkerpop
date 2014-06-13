package com.tinkerpop.gremlin.process.graph.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyValuesTest extends ValuesTest {

    public Traversal<Vertex, Map<String, Object>> get_g_V_values() {
        g.V.values
    }

    public Traversal<Vertex, Map<String, Object>> get_g_V_valuesXname_ageX() {
        g.V.values('name', 'age')
    }

    public Traversal<Edge, Map<String, Object>> get_g_E_valuesXid_label_weightX() {
        g.E.values('id', 'label', 'weight')
    }


    public Traversal<Vertex, Map<String, Object>> get_g_v1_outXcreatedX_values(final Object v1Id) {
        g.v(v1Id).out('created').values

    }
}
