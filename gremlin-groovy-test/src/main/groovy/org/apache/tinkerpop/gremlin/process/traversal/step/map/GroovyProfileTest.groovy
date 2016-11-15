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
package org.apache.tinkerpop.gremlin.process.traversal.step.map

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Bob Briody (http://bobbriody.com
 */
public abstract class GroovyProfileTest {

    public static class Traversals extends ProfileTest {

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_out_out_profile() {
            g.V.out.out.profile() // locked traversal
        }

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_repeatXbothX_timesX3X_profile() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(__.both()).times(3).profile()");
        }

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_whereXinXcreatedX_count_isX1XX_name_profile() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().where(__.in('created').count().is(1l)).values('name').profile()");
        }

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profile() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().sideEffect{Thread.sleep(10)}.sideEffect{Thread.sleep(5)}.profile()")
        }

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_matchXa_created_b__b_in_count_isXeqX1XXX_selectXa_bX_profile() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.match(__.as('a').out('created').as('b'), __.as('b').in.count.is(eq(1))).select('a', 'b').profile()")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_out_profileXmetricsX() {
            g.V.out.out.profile('metrics') // locked traversal
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_profileXmetricsX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(__.both()).times(3).profile('metrics')");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_whereXinXcreatedX_count_isX1XX_name_profileXmetricsX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().where(__.in('created').count().is(1l)).values('name').profile('metrics')");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_sideEffectXThread_sleepX10XX_sideEffectXThread_sleepX5XX_profileXmetricsX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().sideEffect{Thread.sleep(10)}.sideEffect{Thread.sleep(5)}.profile('metrics')")
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__b_in_count_isXeqX1XXX_selectXa_bX_profileXmetricsX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.match(__.as('a').out('created').as('b'), __.as('b').in.count.is(eq(1))).select('a', 'b').profile('metrics')")
        }

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_hasLabelXpersonX_pageRank_byXrankX_byXbothEX_rank_profile() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('person').pageRank.by('rank').by(bothE()).rank.profile()")
        }

        @Override
        public Traversal<Vertex, TraversalMetrics> get_g_V_groupXmX_profile() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.group('m').profile")
        }
    }
}

