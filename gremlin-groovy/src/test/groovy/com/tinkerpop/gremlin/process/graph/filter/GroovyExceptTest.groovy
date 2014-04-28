package com.tinkerpop.gremlin.process.graph.filter

import com.tinkerpop.gremlin.groovy.GremlinLoader
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyExceptTest extends ExceptTest {

    static {
        GremlinLoader.load();
    }

    public Traversal<Vertex, Vertex> get_g_v1_out_exceptXg_v2X(final Object v1Id, final Object v2Id) {
        g.v(v1Id).out.except([g.v(v2Id)])
    }

    public Traversal<Vertex, Vertex> get_g_v1_out_aggregateXxX_out_exceptXxX(final Object v1Id) {
        g.v(v1Id).out.aggregate('x').out.except('x')
    }

    public Traversal<Vertex, String> get_g_v1_outXcreatedX_inXcreatedX_exceptXg_v1X_valueXnameX(final Object v1Id) {
        g.v(v1Id).out('created').in('created').except(g.v(v1Id)).value('name')
    }
}
