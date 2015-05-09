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

import org.apache.tinkerpop.gremlin.process.computer.GroovyTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyWhereTest {

    public static class Traversals extends WhereTest {

        /// where(local)

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX() {
            GroovyTestHelper.compute("g.V.has('age').as('a').out.in.has('age').as('b').select().where('a', eq('b'))", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_neq_bX() {
            GroovyTestHelper.compute("g.V.has('age').as('a').out.in.has('age').as('b').select().where('a', neq('b'))", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX() {
            GroovyTestHelper.compute("g.V.has('age').as('a').out.in.has('age').as('b').select().where(__.as('b').has('name', 'marko'))", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX() {
            GroovyTestHelper.compute("g.V().has('age').as('a').out.in.has('age').as('b').select().where(__.as('a').out('knows').as('b'))", g)
        }

        /// where(global)

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_neq_bX_name(
                final Object v1Id) {
            GroovyTestHelper.compute("g.V(v1Id).as('a').out('created').in('created').as('b').where('a', neq('b')).name", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Object> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX(
                final Object v1Id) {
            GroovyTestHelper.compute("g.V(v1Id).as('a').out('created').in('created').as('b').where(__.as('b').out('created').has('name','ripple')).values('age','name')", g, "v1Id", v1Id)
        }
    }
}
