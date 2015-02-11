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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map

import org.apache.tinkerpop.gremlin.process.Traversal
import org.apache.tinkerpop.gremlin.process.graph.traversal.__
import org.apache.tinkerpop.gremlin.process.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.CountTest
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyCountTest {

    public static class StandardTest extends CountTest {
        @Override
        public Traversal<Vertex, Long> get_g_V_count() {
            g.V.count()
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_out_count() {
            g.V.out.count
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_both_both_count() {
            g.V.both.both.count()
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX3X_count() {
            g.V().repeat(__.out).times(3).count()
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX8X_count() {
            g.V.repeat(__.out).times(8).count()
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_hasXnoX_count() {
            g.V.has('no').count
        }
    }

    public static class ComputerTest extends CountTest {
        @Override
        public Traversal<Vertex, Long> get_g_V_count() {
            ComputerTestHelper.compute("g.V.count()", g)
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_out_count() {
            ComputerTestHelper.compute("g.V.out.count", g)
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_both_both_count() {
            ComputerTestHelper.compute("g.V.both.both.count()", g)
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX3X_count() {
            ComputerTestHelper.compute("g.V().repeat(__.out).times(3).count()", g);
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX8X_count() {
            ComputerTestHelper.compute("g.V.repeat(__.out).times(8).count()", g);
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_hasXnoX_count() {
            ComputerTestHelper.compute("g.V.has('no').count", g)
        }
    }
}
