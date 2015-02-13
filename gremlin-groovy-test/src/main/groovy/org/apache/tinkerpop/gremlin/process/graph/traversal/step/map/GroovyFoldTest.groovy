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

import org.apache.tinkerpop.gremlin.process.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.Traversal
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.FoldTest
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyFoldTest {

    public static class StandardTest extends FoldTest {
        @Override
        public Traversal<Vertex, List<Vertex>> get_g_V_fold() {
            g.V.fold
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_fold_unfold() {
            g.V.fold.unfold
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_foldX0_plusX() {
            g.V.age.fold(0) { seed, age -> seed + age };
        }
    }

    public static class ComputerTest extends FoldTest {
        @Override
        public Traversal<Vertex, List<Vertex>> get_g_V_fold() {
            ComputerTestHelper.compute("g.V.fold", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_fold_unfold() {
            g.engine(StandardTraversalEngine.instance());
            g.V.fold.unfold    // Does not work in OLAP cause fold() is not an endstep.
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_foldX0_plusX() {
            ComputerTestHelper.compute("g.V.age.fold(0) { seed, age -> seed + age }", g);
        }
    }
}
