package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySimplePathTest {

    public static class StandardTest extends SimplePathTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_simplePath(final Object v1Id) {
            g.V(v1Id).out('created').in('created').simplePath
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXboth_simplePathX_timesX3X_path() {
            return g.V.repeat(__.both.simplePath).times(3).path()
        }
    }

    public static class ComputerTest extends SimplePathTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_simplePath(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out('created').in('created').simplePath", g);
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXboth_simplePathX_timesX3X_path() {
            ComputerTestHelper.compute("g.V.repeat(__.both.simplePath).times(3).path()", g);
        }
    }
}
