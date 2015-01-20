package com.tinkerpop.gremlin.process.graph.step.branch

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyUnionTest {

    public static class StandardTest extends UnionTest {

        public Traversal<Vertex, String> get_g_V_unionXout__inX_name() {
            g.V.union(__.out, __.in).name
        }

        public Traversal<Vertex, String> get_g_VX1X_unionXrepeatXoutX_timesX2X__outX_name(final Object v1Id) {
            g.V(v1Id).union(__.repeat(__.out).times(2), __.out).name
        }

        public Traversal<Vertex, String> get_g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX() {
            g.V.choose({ it.label() == 'person' }, __.union(__.out.lang, __.out.name), __.in.label)
        }

        public Traversal<Vertex, Map<String, Long>> get_g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX_groupCount() {
            g.V.choose({ it.label() == 'person' }, __.union(__.out.lang, __.out.name), __.in.label).groupCount
        }

        public Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount() {
            g.V.union(
                    __.repeat(__.union(
                            __.out('created'),
                            __.in('created'))).times(2),
                    __.repeat(__.union(
                            __.in('created'),
                            __.out('created'))).times(2)).label.groupCount()
        }

    }

    public static class ComputerTest extends UnionTest {

        public Traversal<Vertex, String> get_g_V_unionXout__inX_name() {
            ComputerTestHelper.compute("g.V.union(__.out, __.in).name", g)
        }

        public Traversal<Vertex, String> get_g_VX1X_unionXrepeatXoutX_timesX2X__outX_name(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).union(__.repeat(__.out).times(2), __.out).name", g)
        }

        public Traversal<Vertex, String> get_g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX() {
            ComputerTestHelper.compute("g.V.choose({ it.label() == 'person' }, __.union(__.out.lang, __.out.name), __.in.label)", g)
        }

        public Traversal<Vertex, Map<String, Long>> get_g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX_groupCount() {
            ComputerTestHelper.compute("g.V.choose({ it.label() == 'person' }, __.union(__.out.lang, __.out.name), __.in.label).groupCount", g)
        }


        public Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount() {
            ComputerTestHelper.compute("""
            g.V.union(
                    __.repeat(__.union(
                            __.out('created'),
                            __.in('created'))).times(2),
                    __.repeat(__.union(
                            __.in('created'),
                            __.out('created'))).times(2)).label.groupCount()
           """, g)
        }
    }
}
