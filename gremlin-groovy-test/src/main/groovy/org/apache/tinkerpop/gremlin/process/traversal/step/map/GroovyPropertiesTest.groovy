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
import org.apache.tinkerpop.gremlin.structure.VertexProperty

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyPropertiesTest {

    public static class Traversals extends PropertiesTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('age').properties('name', 'age').value")
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('age').properties('age', 'name').value")
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_properties_hasXid_nameIdX_value(final Object nameId) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('age').properties().has(T.id, nameId).value()", "nameId", nameId)
        }

        @Override
        public Traversal<Vertex, VertexProperty<String>> get_g_V_hasXageX_propertiesXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('age').properties('name')")
        }

        @Override
        public Traversal<VertexProperty<String>, String> get_g_injectXg_VX1X_propertiesXnameX_nextX_value(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.inject(g.V(v1Id).properties('name').next()).value()", "v1Id", v1Id)
        }
    }

}
