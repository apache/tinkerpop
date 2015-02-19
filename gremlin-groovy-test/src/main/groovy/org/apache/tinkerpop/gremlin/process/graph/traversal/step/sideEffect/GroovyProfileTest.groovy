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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect

import org.apache.tinkerpop.gremlin.process.Traversal
import org.apache.tinkerpop.gremlin.process.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.TraversalEngine
import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.ProfileTest
import org.apache.tinkerpop.gremlin.process.util.metric.StandardTraversalMetrics
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.process.graph.traversal.__
import org.junit.Ignore

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyProfileTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTest extends ProfileTest {

        @Override
        Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile() {
            g.V.out.out.profile();
        }

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_repeat_both_profile() {
            g.V.repeat(__.both()).times(3).profile();
        }

        @Override
        @org.junit.Ignore
        void testProfileTimes() {
        }

        @Override
        Traversal<Vertex, StandardTraversalMetrics> get_g_V_sleep_sleep_profile() {
            return null
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTest extends ProfileTest {

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile() {
            ComputerTestHelper.compute("g.V.out.out.profile()", g);
        }


        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_repeat_both_profile() {
           ComputerTestHelper.compute("g.V.repeat(__.both()).times(3).profile()", g);
        }

        @Override
        @org.junit.Ignore
        void testProfileTimes() {
        }

        @Override
        Traversal<Vertex, StandardTraversalMetrics> get_g_V_sleep_sleep_profile() {
            return null
        }
    }
}

