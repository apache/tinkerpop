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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyFilterTest {

    public static class Traversals extends FilterTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXfalseX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.filter { false }");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXtrueX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.filter { true }");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.filter { it.property('lang').orElse('none') == 'java' }");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_filterXage_gt_30X(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).filter { it.age > 30 }", "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_filterXage_gt_30X(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out.filter { it.property('age').orElse(0) > 30 }", "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.filter { it.name.startsWith('m') || it.name.startsWith('p') }");
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXfalseX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.E.filter { false }");
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXtrueX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.E.filter { true }");
        }
    }
}
