/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyGroupCountTest {

    public static class Traversals extends GroupCountTest {

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_groupCount_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out('created').groupCount.by('name')")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_groupCountXaX_byXnameX_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out('created').groupCount('a').by('name').cap('a')")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCount() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out('created').name.groupCount")
        }

        @Override
        public Traversal<Vertex, Map<Vertex, Long>> get_g_V_outXcreatedX_groupCountXxX_capXxX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out('created').groupCount('x').cap('x')")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCountXaX_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out('created').name.groupCount('a').cap('a')")
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_hasXnoX_groupCount() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('no').groupCount")
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_hasXnoX_groupCountXaX_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('no').groupCount('a').cap('a')")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(__.out.groupCount('a').by('name')).times(2).cap('a')")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
            g.V.union(
                    __.repeat(__.out).times(2).groupCount('m').by('lang'),
                    __.repeat(__.in).times(2).groupCount('m').by('name')).cap('m')
            """)
        }

        @Override
        public Traversal<Vertex, Map<Long, Long>> get_g_V_groupCount_byXbothE_countX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.groupCount.by(bothE().count)")
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_unionXoutXknowsX__outXcreatedX_inXcreatedXX_groupCount_selectXvaluesX_unfold_sum() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.union(out('knows'), out('created').in('created')).groupCount.select(values).unfold.sum")
        }

        @Override
        public Traversal<Vertex, Map<Vertex, Long>> get_g_V_both_groupCountXaX_out_capXaX_selectXkeysX_unfold_both_groupCountXaX_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.both.groupCount('a').out.cap('a').select(keys).unfold.both.groupCount('a').cap('a')")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_groupCountXaX_byXlabelX_asXbX_barrier_whereXselectXaX_selectXsoftwareX_isXgtX2XXX_selectXbX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.both.groupCount('a').by(label).as('b').barrier.where(select('a').select('software').is(gt(2))).select('b').name")
        }
    }
}
