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
public abstract class GroovyGroupTest {

    public static class Traversals extends GroupTest {

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.group.by('name')")
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX_by() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.group.by('name').by")
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_groupXaX_byXnameX_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.group('a').by('name').cap('a')")
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('lang').group('a').by('lang').by('name').out.cap('a')")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_hasXlangX_group_byXlangX_byXcountX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('lang').group.by('lang').by(count())")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupXaX_byXnameX_byXcountX_timesX2X_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(__.out.group('a').by('name').by(count())).times(2).cap('a')")
        }

        @Override
        public Traversal<Vertex, Map<Long, Collection<String>>> get_g_V_group_byXoutE_countX_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.group.by(__.outE.count).by('name')")
        }

        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_groupXaX_byXlabelX_byXoutE_weight_sumX_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.group('a').by(label).by(outE().weight.sum).cap('a')");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(both('followedBy')).times(2).group.by('songType').by(count())")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byXcountX_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(both('followedBy')).times(2).group('a').by('songType').by(count()).cap('a')")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_group_byXname_substring_1X_byXconstantX1XX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.group.by{it.name[0]}.by(constant(1l))")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_groupXaX_byXname_substring_1X_byXconstantX1XX_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.group('a').by{it.name[0]}.by(constant(1l)).cap('a')")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_group_byXlabelX_selectXpersonX_unfold_outXcreatedX_name_limitX2X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out.group.by(label).select('person').unfold.out('created').name.limit(2)")
        }

        @Override
        public Traversal<Vertex, Map<String, Map<String, Long>>> get_g_V_hasLabelXsongX_group_byXnameX_byXproperties_groupCount_byXlabelXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('song').group.by('name').by(__.properties().groupCount.by(label))")
        }

        @Override
        public Traversal<Vertex, Map<String, Map<String, Long>>> get_g_V_hasLabelXsongX_groupXaX_byXnameX_byXproperties_groupCount_byXlabelXX_out_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('song').group('a').by('name').by(__.properties().groupCount.by(label)).out.cap('a')")
        }

        @Override
        public Traversal<Vertex, Map<String, Map<Object, Object>>> get_g_V_repeatXunionXoutXknowsX_groupXaX_byXageX__outXcreatedX_groupXbX_byXnameX_byXcountXX_groupXaX_byXnameXX_timesX2X_capXa_bX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(union(out('knows').group('a').by('age'), out('created').group('b').by('name').by(count())).group('a').by('name')).times(2).cap('a', 'b')")
        }

        @Override
        public Traversal<Vertex, Map<Long, Map<String, List<Vertex>>>> get_g_V_group_byXbothE_countX_byXgroup_byXlabelXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.group().by(bothE().count).by(group().by(label))")
        }

        @Override
        public Traversal<Vertex, Map<String, Map<String, Number>>> get_g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out('followedBy').group.by('songType').by(bothE().group.by(label).by(values('weight').sum))")
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_groupXmX_byXnameX_byXinXknowsX_nameX_capXmX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.group('m').by('name').by(__.in('knows').name).cap('m')")
        }

        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_group_byXlabelX_byXbothE_groupXaX_byXlabelX_byXweight_sumX_weight_sumX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.group().by(label).by(bothE().group('a').by(label).by(values('weight').sum).weight.sum)")
        }
    }
}
