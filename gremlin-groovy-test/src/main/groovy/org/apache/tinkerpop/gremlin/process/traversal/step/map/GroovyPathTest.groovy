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

import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyPathTest {

    public static class Traversals extends PathTest {

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_name_path(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).name.path", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_out_path_byXageX_byXnameX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out.path.by('age').by('name')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_timesX2X_path_by_byXnameX_byXlangX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(__.out).times(2).path.by.by('name').by('lang')")
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_out_path_byXnameX_byXageX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out.out.path.by('name').by('age')")
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXaX_hasXname_markoX_asXbX_hasXage_29X_asXcX_path() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').has('name', 'marko').as('b').has('age', 29).as('c').path")
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_outEXcreatedX_inV_inE_outV_path(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).outE('created').inV.inE.outV.path()", "v1Id", v1Id)
        }
    }
}
