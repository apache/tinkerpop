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
import com.tinkerpop.gremlin.process.ComputerTestHelper
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroupTest
import com.tinkerpop.gremlin.structure.Vertex

import com.tinkerpop.gremlin.process.graph.traversal.__
/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyGroupTest {

    public static class StandardTest extends GroupTest {

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX() {
            g.V.group.by('name')
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX() {
            g.V.has('lang').group('a').by('lang').by('name').out.cap('a')
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_group_byXlangX_byX1X_byXsizeX() {
            g.V.has('lang').group.by('lang').by { 1 }.by { it.size() }
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_repeatXout_groupXaX_byXnameX_by_byXsizeXX_timesX2X_capXaX() {
            g.V.repeat(__.out.group('a').by('name').by.by { it.size() }).times(2).cap('a')
        }

        @Override
        public Traversal<Vertex, Map<Long, Collection<String>>> get_g_V_group_byXoutE_countX_byXnameX() {
            g.V.group.by(__.outE.count).by('name')
        }
    }

    public static class ComputerTest extends GroupTest {

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX() {
            ComputerTestHelper.compute("g.V.group.by('name')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX() {
            ComputerTestHelper.compute("g.V.has('lang').group('a').by('lang').by('name').out.cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_hasXlangX_group_byXlangX_byX1X_byXsizeX() {
            ComputerTestHelper.compute("g.V.has('lang').group.by('lang').by{1}.by{it.size()}", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Integer>> get_g_V_repeatXout_groupXaX_byXnameX_by_byXsizeXX_timesX2X_capXaX() {
            ComputerTestHelper.compute("g.V.repeat(__.out.group('a').by('name').by.by { it.size() }).times(2).cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<Long, Collection<String>>> get_g_V_group_byXoutE_countX_byXnameX() {
            ComputerTestHelper.compute("g.V.group.by(__.outE.count).by('name')", g)
        }
    }
}
