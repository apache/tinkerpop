package com.tinkerpop.gremlin.process.graph.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyAggregateTestImpl extends AggregateTest {

    public Traversal<Vertex, Vertex> get_g_v1_aggregateXaX_outXcreatedX_inXcreatedX_exceptXaX(final Object v1Id) {
        g.v(v1Id).aggregate('a').out('created').in('created').except('a')
    }

    public List<String> get_g_V_valueXnameX_aggregateXaX_iterate_getXaX() {
        g.V().value('name').aggregate('a').iterate().memory().get('a')
    }

    public List<String> get_g_V_aggregateXa_nameX_iterate_getXaX() {
        g.V.aggregate('a') { it['name'] }.iterate().memory().get('a')
    }
}
