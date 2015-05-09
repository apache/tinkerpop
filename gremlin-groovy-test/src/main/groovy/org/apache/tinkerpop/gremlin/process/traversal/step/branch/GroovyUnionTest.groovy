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

import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalScriptHelper
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyUnionTest {

    public static class Traversals extends UnionTest {

        public Traversal<Vertex, String> get_g_V_unionXout__inX_name() {
            TraversalScriptHelper.compute("g.V.union(__.out, __.in).name", g)
        }

        public Traversal<Vertex, String> get_g_VX1X_unionXrepeatXoutX_timesX2X__outX_name(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).union(repeat(__.out).times(2), __.out).name", g, "v1Id", v1Id)
        }

        public Traversal<Vertex, String> get_g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX() {
            TraversalScriptHelper.compute("g.V.choose(__.label.is('person'), union(__.out.lang, __.out.name), __.in.label)", g)
        }

        public Traversal<Vertex, Map<String, Long>> get_g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX_groupCount() {
            TraversalScriptHelper.compute("g.V.choose(__.label.is('person'), union(__.out.lang, __.out.name), __.in.label).groupCount", g)
        }

        public Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount() {
            TraversalScriptHelper.compute("""
            g.V.union(
                    repeat(union(
                            out('created'),
                            __.in('created'))).times(2),
                    repeat(union(
                            __.in('created'),
                            out('created'))).times(2)).label.groupCount()
           """, g)
        }

        @Override
        public Traversal<Vertex, Number> get_g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX(
                final Object v1Id, final Object v2Id) {
            TraversalScriptHelper.compute("g.V(v1Id, v2Id).union(outE().count, inE().count, outE().weight.sum)", g, "v1Id", v1Id, "v2Id", v2Id);
        }
    }
}
