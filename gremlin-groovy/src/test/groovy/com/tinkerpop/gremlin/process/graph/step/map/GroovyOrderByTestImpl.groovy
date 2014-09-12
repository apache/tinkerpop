package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyOrderByTestImpl extends OrderByTest {

    @Override
    public Traversal<Vertex, String> get_g_V_orderByXname_incrX_name() {
        g.V().orderBy('name', T.incr).value('name');
    }

    @Override
    public Traversal<Vertex, String> get_g_V_orderByXnameX_name() {
        g.V().orderBy('name').value("name");
    }

    @Override
    public Traversal<Vertex, Double> get_g_V_outE_orderByXweight_decrX_weight() {
        g.V().outE().orderBy('weight', T.decr).value("weight");
    }
}
