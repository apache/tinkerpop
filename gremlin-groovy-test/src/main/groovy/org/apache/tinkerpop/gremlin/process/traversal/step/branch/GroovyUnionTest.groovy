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
package org.apache.tinkerpop.gremlin.process.traversal.step.branch

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyUnionTest {

    public static class Traversals extends UnionTest {

        @Override
        public Traversal<Vertex, String> get_g_V_unionXout__inX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.union(__.out, __.in).name")
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_unionXrepeatXoutX_timesX2X__outX_name(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).union(repeat(__.out).times(2), __.out).name", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.choose(__.label.is('person'), union(__.out.lang, __.out.name), __.in.label)")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX_groupCount() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.choose(__.label.is('person'), union(__.out.lang, __.out.name), __.in.label).groupCount")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
            g.V.union(
                    repeat(union(
                            out('created'),
                            __.in('created'))).times(2),
                    repeat(union(
                            __.in('created'),
                            out('created'))).times(2)).label.groupCount()
           """)
        }

        @Override
        public Traversal<Vertex, Number> get_g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX(
                final Object v1Id, final Object v2Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id, v2Id).union(outE().count, inE().count, outE().weight.sum)", "v1Id", v1Id, "v2Id", v2Id);
        }

        @Override
        public Traversal<Vertex, Number> get_g_VX1_2X_localXunionXoutE_count__inE_count__outE_weight_sumXX(
                final Object v1Id, final Object v2Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id, v2Id).local(union(outE().count, inE().count, outE().weight.sum))", "v1Id", v1Id, "v2Id", v2Id);
        }

        @Override
        public Traversal<Vertex, Number> get_g_VX1_2X_localXunionXcountXX(
                final Object v1Id, final Object v2Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id, v2Id).local(union(count()))", "v1Id", v1Id, "v2Id", v2Id);
        }
    }
}
