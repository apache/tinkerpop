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
import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.UseEngine
import static org.apache.tinkerpop.gremlin.structure.P.*;
import org.apache.tinkerpop.gremlin.structure.Vertex

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.junit.Test

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyWhereTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends WhereTest {
        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX() {
            g.V.has('age').as('a').out.in.has('age').as('b').select().where('a', eq('b'));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_neq_bX() {
            g.V.has('age').as('a').out.in.has('age').as('b').select().where('a',neq('b'));
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

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends WhereTest {
        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eq_bX() {
            ComputerTestHelper.compute("g.V.has('age').as('a').out.in.has('age').as('b').select().where('a', eq('b'))", g);
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_neq_bX() {
            ComputerTestHelper.compute("g.V.has('age').as('a').out.in.has('age').as('b').select().where('a', neq('b'))", g);
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX() {
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX() {
        }

        @Override
        Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX() {
            // override with nothing until the test itself is supported
            return null
        }
    }
}
