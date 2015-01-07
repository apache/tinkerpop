package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyCyclicPathTest {

    public static class StandardTest extends CyclicPathTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_cyclicPath(final Object v1Id) {
            g.V(v1Id).out('created').in('created').cyclicPath
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_outXcreatedX_inXcreatedX_cyclicPath_path(final Object v1Id) {
            g.V(v1Id).out('created').in('created').cyclicPath.path
        }
    }

    public static class ComputerTest extends CyclicPathTest {

        @Override
        Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_cyclicPath(final Object v1) {
            ComputerTestHelper.compute("g.V(${v1}).out('created').in('created').cyclicPath", g);
        }

        @Override
        Traversal<Vertex, Path> get_g_VX1X_outXcreatedX_inXcreatedX_cyclicPath_path(final Object v1) {
            ComputerTestHelper.compute("g.V(${v1}).out('created').in('created').cyclicPath().path()", g);
        }
    }
}
