package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyGroupCountTest {

    public static class StandardTest extends GroupCountTest {

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_groupCount_byXnameX() {
            g.V.out('created').groupCount.by('name')
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCount() {
            g.V.out('created').name.groupCount
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCountXaX() {
            g.V.out('created').name.groupCount('a')
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_filterXfalseX_groupCount() {
            g.V.filter { false }.groupCount;
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX() {
            g.V.repeat(__.out.groupCount('a').by('name')).times(2).cap('a')
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX() {
            g.V.union(
                    __.repeat(__.out).times(2).groupCount('m').by('lang'),
                    __.repeat(__.in).times(2).groupCount('m').by('name')).cap('m')
        }
    }

    public static class ComputerTest extends GroupCountTest {

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_groupCount_byXnameX() {
            ComputerTestHelper.compute("g.V.out('created').groupCount.by('name')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCount() {
            ComputerTestHelper.compute("g.V.out('created').name.groupCount", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCountXaX() {
            ComputerTestHelper.compute("g.V.out('created').name.groupCount('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_filterXfalseX_groupCount() {
            ComputerTestHelper.compute("g.V.filter { false }.groupCount", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX() {
            ComputerTestHelper.compute("g.V.repeat(__.out.groupCount('a').by('name')).times(2).cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX() {
            ComputerTestHelper.compute("""
            g.V.union(
                    __.repeat(__.out).times(2).groupCount('m').by('lang'),
                    __.repeat(__.in).times(2).groupCount('m').by('name')).cap('m')
            """, g)
        }
    }
}
