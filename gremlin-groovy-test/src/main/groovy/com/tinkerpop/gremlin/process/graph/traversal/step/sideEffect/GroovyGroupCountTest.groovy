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
package com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.traversal.__
import com.tinkerpop.gremlin.process.ComputerTestHelper
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroupCountTest
import com.tinkerpop.gremlin.structure.Vertex

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
