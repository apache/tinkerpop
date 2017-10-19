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
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyInjectTest {

    public static class Traversals extends InjectTest {
        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_injectXv2X_name(final Object v1Id, final Object v2Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out.inject(g.V(v2Id).next()).name", "v1Id", v1Id, "v2Id", v2Id);
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out().name.inject('daniel').as('a').map { it.length() }.path", "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_injectXg_VX4XX_out_name(final Object v1Id, final Object v4Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).inject(g.V(v4Id).next()).out.name", "v1Id", v1Id, "v4Id", v4Id)
        }
    }
}
