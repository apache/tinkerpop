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

import org.apache.tinkerpop.gremlin.process.*
import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.Scope
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyGroupTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends GroupTest {

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX() {
            g.V.group.by('name')
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_groupXaX_byXnameX_capXaX() {
            g.V.group('a').by('name').cap('a')
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX() {
            g.V.has('lang').group('a').by('lang').by('name').out.cap('a')
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_hasXlangX_group_byXlangX_byX1X_byXcountXlocalXX() {
            g.V.has('lang').group.by('lang').by(__.inject(1)).by(__.count(Scope.local));
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupXaX_byXnameX_by_byXcountXlocalXX_timesX2X_capXaX() {
            g.V.repeat(__.out.group('a').by('name').by.by(__.count(Scope.local))).times(2).cap('a')
        }

        @Override
        public Traversal<Vertex, Map<Long, Collection<String>>> get_g_V_group_byXoutE_countX_byXnameX() {
            g.V.group.by(__.outE.count).by('name')
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends GroupTest {

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX() {
            ComputerTestHelper.compute("g.V.group.by('name')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_groupXaX_byXnameX_capXaX() {
            ComputerTestHelper.compute("g.V.group('a').by('name').cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX() {
            ComputerTestHelper.compute("g.V.has('lang').group('a').by('lang').by('name').out.cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_hasXlangX_group_byXlangX_byX1X_byXcountXlocalXX() {
            ComputerTestHelper.compute("g.V.has('lang').group.by('lang').by(__.inject(1)).by(__.count(Scope.local))", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupXaX_byXnameX_by_byXcountXlocalXX_timesX2X_capXaX() {
            ComputerTestHelper.compute("g.V.repeat(__.out.group('a').by('name').by.by(__.count(Scope.local))).times(2).cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<Long, Collection<String>>> get_g_V_group_byXoutE_countX_byXnameX() {
            ComputerTestHelper.compute("g.V.group.by(__.outE.count).by('name')", g)
        }
    }
}
