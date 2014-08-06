package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyAggregateTestImpl extends AggregateTest {

    public Traversal<Vertex, Vertex> get_g_v1_aggregateXaX_outXcreatedX_inXcreatedX_exceptXaX(final Object v1Id) {
        g.v(v1Id).aggregate.as('a').out('created').in('created').except('a')
    }

    public Traversal<Vertex, List<String>> get_g_V_valueXnameX_aggregate() {
        g.V().value('name').aggregate()
    }

    public Traversal<Vertex, List<String>> get_g_V_aggregateXnameX() {
        g.V.aggregate() { it.value('name') }
    }

    public Traversal<Vertex, Path> get_g_V_out_aggregate_asXaX_path() {
        return g.V().out().aggregate.as('a').path();
    }
}
