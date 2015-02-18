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

import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest
import org.apache.tinkerpop.gremlin.process.T
import org.apache.tinkerpop.gremlin.process.Traversal
import org.apache.tinkerpop.gremlin.process.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.TraversalEngine
import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.TreeTest
import org.apache.tinkerpop.gremlin.process.graph.util.Tree
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.junit.Test

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyTreeTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTest extends TreeTest {

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_tree_byXidX() {
            g.V.out.out.tree.by(T.id);
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_byXidX() {
            g.V.out.out.tree('a').by(T.id);
        }

        @Override
        public Traversal<Vertex, Tree> get_g_VX1X_out_out_tree_byXnameX(final Object v1Id) {
            g.V(v1Id).out.out.tree.by('name');
        }

        @Override
        public Traversal<Vertex, Tree> get_g_VX1X_out_out_treeXaX_byXnameX_both_both_capXaX(final Object v1Id) {
            g.V(v1Id).out.out.tree('a').by('name').both.both.cap('a');
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX() {
            g.V.out.out.tree("a");
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTest extends TreeTest {

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_tree_byXidX() {
            ComputerTestHelper.compute("g.V.out.out.tree.by(id)", g)
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_byXidX() {
            ComputerTestHelper.compute("g.V.out.out.tree('a').by(id)", g)
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX() {
            ComputerTestHelper.compute("g.V.out.out.tree('a')", g)
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        void g_VX1X_out_out_tree_byXnameX() {

        }

        @Override
        Traversal<Vertex, Tree> get_g_VX1X_out_out_tree_byXnameX(Object v1Id) {
            return null
        }

        @Override
        Traversal<Vertex, Tree> get_g_VX1X_out_out_treeXaX_byXnameX_both_both_capXaX(Object v1Id) {
            return null
        }
    }
}
