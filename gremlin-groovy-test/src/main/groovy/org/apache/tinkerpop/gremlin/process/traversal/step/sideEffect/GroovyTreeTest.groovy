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
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.junit.Test

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyTreeTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends TreeTest {

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_tree_byXidX() {
            g.V.out.out.tree.by(T.id);
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_byXidX_capXaX() {
            g.V.out.out.tree('a').by(T.id).cap('a');
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
        public Traversal<Vertex, Tree> get_g_V_out_out_tree() {
            g.V.out.out.tree();
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_capXaX() {
            g.V.out.out.tree('a').cap('a');
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends TreeTest {

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_tree_byXidX() {
            ComputerTestHelper.compute("g.V.out.out.tree.by(id)", g)
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_byXidX_capXaX() {
            ComputerTestHelper.compute("g.V.out.out.tree('a').by(id).cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_tree() {
            ComputerTestHelper.compute("g.V.out.out.tree()", g)
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_capXaX() {
            ComputerTestHelper.compute("g.V.out.out.tree('a').cap('a')", g)
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
