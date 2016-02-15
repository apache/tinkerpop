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
@Deprecated
public abstract class GroovyGroupTestV3d0 {

    public static class Traversals extends GroupTestV3d0 {

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.groupV3d0.by('name')")
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_groupXaX_byXnameX_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.groupV3d0('a').by('name').cap('a')")
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('lang').groupV3d0('a').by('lang').by('name').out.cap('a')")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_hasXlangX_group_byXlangX_byX1X_byXcountXlocalXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('lang').groupV3d0.by('lang').by(__.inject(1)).by(__.count(Scope.local))")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupXaX_byXnameX_by_byXcountXlocalXX_timesX2X_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(__.out.groupV3d0('a').by('name').by.by(__.count(Scope.local))).times(2).cap('a')")
        }

        @Override
        public Traversal<Vertex, Map<Long, Collection<String>>> get_g_V_group_byXoutE_countX_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.groupV3d0.by(__.outE.count).by('name')")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byX1X_byXcountXlocalXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(both('followedBy')).times(2).groupV3d0.by('songType').by(inject(1)).by(count(local))")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byX1X_byXcountXlocalXX_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(both('followedBy')).times(2).groupV3d0('a').by('songType').by(inject(1)).by(count(local)).cap('a')")
        }
    }
}
