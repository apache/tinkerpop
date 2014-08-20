package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GroovyCyclicPathTestImpl extends CyclicPathTest {
    @Override
    public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_cyclicPath(final Object v1Id) {
        g.v(v1Id).out("created").in("created").cyclicPath
    }

    @Override
    public Traversal<Vertex, Path> get_g_v1_outXcreatedX_inXcreatedX_cyclicPath_path(final Object v1Id) {
        g.v(v1Id).out("created").in("created").cyclicPath.path
    }
}
