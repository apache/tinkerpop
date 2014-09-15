package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyValueMapTestImpl extends ValueMapTest {

    @Override
    public Traversal<Vertex, Map<String, List>> get_g_V_valueMap() {
        g.V.valueMap
    }

    @Override
    public Traversal<Vertex, Map<String, List>> get_g_V_valueMapXname_ageX() {
        g.V.valueMap('name', 'age')
    }

    @Override
    public Traversal<Edge, Map<String, Object>> get_g_E_valueMapXid_label_weightX() {
        g.E.valueMap('id', 'label', 'weight')
    }

    @Override
    public Traversal<Vertex, Map<String, List<String>>> get_g_v1_outXcreatedX_valueMap(final Object v1Id) {
        g.v(v1Id).out('created').valueMap
    }
}
