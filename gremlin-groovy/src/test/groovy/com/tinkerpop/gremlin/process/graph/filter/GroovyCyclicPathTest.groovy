package com.tinkerpop.gremlin.process.graph.filter

import com.tinkerpop.gremlin.groovy.GremlinLoader
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyCyclicPathTest extends CyclicPathTest {

    static {
        GremlinLoader.load();
    }

    public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_cyclicPath(final Object v1Id) {
        g.v(v1Id).out("created").in("created").cyclicPath
    }
}
