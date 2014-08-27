package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyAggregateTestImpl extends AggregateTest {

    @Override
    public Traversal<Vertex, List<String>> get_g_V_valueXnameX_aggregate() {
        g.V().value('name').aggregate()
    }

    @Override
    public Traversal<Vertex, List<String>> get_g_V_aggregateXnameX() {
        g.V().aggregate { it.get().value('name') }
    }

    @Override
    public Traversal<Vertex, Path> get_g_V_out_aggregateXaX_path() {
        g.V().out().aggregate('a').path();
    }
}
