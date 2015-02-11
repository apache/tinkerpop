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
package com.tinkerpop.gremlin.process.graph.traversal.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.ComputerTestHelper
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.WhereTest
import com.tinkerpop.gremlin.structure.Compare
import com.tinkerpop.gremlin.structure.Vertex

import com.tinkerpop.gremlin.process.graph.traversal.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyWhereTest {

    public static class StandardTest extends WhereTest {
        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX() {
            g.V.has('age').as('a').out.in.has('age').as('b').select().where('a', Compare.eq, 'b');
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_neq_bX() {
            g.V.has('age').as('a').out.in.has('age').as('b').select().where('a',Compare.neq, 'b');
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX() {
            g.V.has('age').as('a').out.in.has('age').as('b').select().where(__.as('b').has('name', 'marko'));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX() {
            g.V().has('age').as('a').out.in.has('age').as('b').select().where(__.as('a').out('knows').as('b'));
        }
    }

    public static class ComputerTest extends WhereTest {
        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX() {
            ComputerTestHelper.compute("g.V.has('age').as('a').out.in.has('age').as('b').select().where('a', Compare.eq, 'b')", g);
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_neq_bX() {
            ComputerTestHelper.compute("g.V.has('age').as('a').out.in.has('age').as('b').select().where('a', Compare.neq, 'b')", g);
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX() {
            // TODO: ComputerTestHelper.compute("g.V.has('age').as('a').out.in.has('age').as('b').select().where(__.as('b').has('name', 'marko'))", g);
            g.V.has('age').as('a').out.in.has('age').as('b').select().where(__.as('b').has('name', 'marko'));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX() {
            // TODO: ComputerTestHelper.compute("g.V().has('age').as('a').out.in.has('age').as('b').select().where(__.as('a').out('knows').as('b'))", g);
            g.V().has('age').as('a').out.in.has('age').as('b').select().where(__.as('a').out('knows').as('b'));
        }
    }
}
