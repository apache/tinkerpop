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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyTreeTest {

    public static class Traversals extends TreeTest {

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_tree_byXidX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out.out.tree.by(id)")
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_byXidX_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out.out.tree('a').by(id).cap('a')")
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_tree() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out.out.tree()")
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out.out.tree('a').cap('a')")
        }

        @Override
        public Traversal<Vertex, Tree> get_g_VX1X_out_out_treeXaX_byXnameX_both_both_capXaX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out.out.tree('a').by('name').both.both.cap('a')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Tree> get_g_VX1X_out_out_tree_byXnameX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out.out.tree.by('name')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_out_tree() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out.out.out.tree")
        }

    }
}
