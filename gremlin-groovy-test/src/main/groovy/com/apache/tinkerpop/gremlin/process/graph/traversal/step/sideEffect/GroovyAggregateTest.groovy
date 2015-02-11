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
package com.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect

import com.apache.tinkerpop.gremlin.process.Path
import com.apache.tinkerpop.gremlin.process.Traversal
import com.apache.tinkerpop.gremlin.process.ComputerTestHelper
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.AggregateTest
import com.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyAggregateTest {

    public static class StandardTest extends AggregateTest {

        @Override
        public Traversal<Vertex, List<String>> get_g_V_name_aggregate() {
            g.V.name.aggregate
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_aggregate_byXnameX() {
            g.V.aggregate.by('name')
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_aggregateXaX_path() {
            g.V.out.aggregate('a').path;
        }
    }

    public static class ComputerTest extends AggregateTest {

        @Override
        public Traversal<Vertex, List<String>> get_g_V_name_aggregate() {
            ComputerTestHelper.compute("g.V.name.aggregate", g)
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_aggregate_byXnameX() {
            ComputerTestHelper.compute("g.V.aggregate.by('name')", g)
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_aggregateXaX_path() {
            ComputerTestHelper.compute("g.V.out.aggregate('a').path", g)
        }
    }
}
