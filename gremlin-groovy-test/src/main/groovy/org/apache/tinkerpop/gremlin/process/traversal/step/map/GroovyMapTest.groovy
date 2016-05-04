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
package org.apache.tinkerpop.gremlin.process.traversal.step.map

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class GroovyMapTest {

    public static class Traversals extends MapTest {

        @Override
        public Traversal<Vertex, String> get_g_VX1X_mapXnameX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).map { v -> v.name }", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Integer> get_g_VX1X_outE_label_mapXlengthX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).outE.label.map { l -> l.length() }", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Integer> get_g_VX1X_out_mapXnameX_mapXlengthX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out.map { v -> v.name }.map { n -> n.length() }", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, String> get_g_withPath_V_asXaX_out_mapXa_nameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.withPath().V.as('a').out.map { v -> v.path('a').name }")
        }

        @Override
        public Traversal<Vertex, String> get_g_withPath_V_asXaX_out_out_mapXa_name_it_nameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.withPath().V().as('a').out.out().map { v -> v.path('a').name + v.name }")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_mapXselectXaXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').map(select('a'))")
        }
    }
}
