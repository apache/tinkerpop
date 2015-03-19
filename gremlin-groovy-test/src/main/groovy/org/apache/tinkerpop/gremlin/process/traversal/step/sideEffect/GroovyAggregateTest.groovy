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

import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyAggregateTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends AggregateTest {

        @Override
        public Traversal<Vertex, List<String>> get_g_V_name_aggregateXxX_capXxX() {
            g.V.name.aggregate('x').cap('x')
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_aggregateXxX_byXnameX_capXxX() {
            g.V.aggregate('x').by('name').cap('x')
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_aggregateXaX_path() {
            g.V.out.aggregate('a').path;
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends AggregateTest {

        @Override
        public Traversal<Vertex, List<String>> get_g_V_name_aggregateXxX_capXxX() {
            ComputerTestHelper.compute("g.V.name.aggregate('x').cap('x')", g)
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_aggregateXxX_byXnameX_capXxX() {
            ComputerTestHelper.compute("g.V.aggregate('x').by('name').cap('x')", g)
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_aggregateXaX_path() {
            ComputerTestHelper.compute("g.V.out.aggregate('a').path", g)
        }
    }
}
