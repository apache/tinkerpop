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
import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.process.traversal.util.StandardTraversalMetrics
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyProfileTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends ProfileTest {

        @Override
        Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile() {
            g.V.out.out.profile();
        }

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_repeat_both_profile() {
            g.V.repeat(__.both()).times(3).profile();
        }

        @Override
        Traversal<Vertex, StandardTraversalMetrics> get_g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX_profile() {
            g.V().has(__.in("created").count().is(1l)).values("name").profile();
        }

        @org.junit.Ignore
        @Override
        void g_V_sleep_sleep_profile() {
        }

        @Override
        Traversal<Vertex, StandardTraversalMetrics> get_g_V_sleep_sleep_profile() {
            return null
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends ProfileTest {

        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_out_out_profile() {
            ComputerTestHelper.compute("g.V.out.out.profile()", g);
        }


        @Override
        public Traversal<Vertex, StandardTraversalMetrics> get_g_V_repeat_both_profile() {
            ComputerTestHelper.compute("g.V.repeat(__.both()).times(3).profile()", g);
        }

        @Override
        Traversal<Vertex, StandardTraversalMetrics> get_g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX_profile() {
            ComputerTestHelper.compute(" g.V().has(__.in('created').count().is(1l)).values('name').profile()", g);
        }

        @Override
        @org.junit.Ignore
        void g_V_sleep_sleep_profile() {
        }

        @Override
        Traversal<Vertex, StandardTraversalMetrics> get_g_V_sleep_sleep_profile() {
            return null
        }
    }
}

