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

import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalScriptHelper
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyWhereTest {

    public static class Traversals extends WhereTest {

        /// where(local)

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_eqXbXX() {
            TraversalScriptHelper.compute("g.V.has('age').as('a').out.in.has('age').as('b').select().where('a', eq('b'))", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_neqXbXX() {
            TraversalScriptHelper.compute("g.V.has('age').as('a').out.in.has('age').as('b').select().where('a', neq('b'))", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXb_hasXname_markoXX() {
            TraversalScriptHelper.compute("g.V.has('age').as('a').out.in.has('age').as('b').select().where(__.as('b').has('name', 'marko'))", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_select_whereXa_outXknowsX_bX() {
            TraversalScriptHelper.compute("g.V().has('age').as('a').out.in.has('age').as('b').select().where(__.as('a').out('knows').as('b'))", g)
        }

        /// where(global)

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_neqXbXX_name(
                final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).as('a').out('created').in('created').as('b').where('a', neq('b')).name", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Object> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX(
                final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).as('a').out('created').in('created').as('b').where(__.as('b').out('created').has('name','ripple')).values('age','name')", g, "v1Id", v1Id)
        }

        // except/retain functionality

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXeqXaXX_name(
                final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).as('a').out('created').in('created').where(eq('a')).name", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_name(
                final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).as('a').out('created').in('created').where(neq('a')).name", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_aggregateXxX_out_whereXwithoutXaXX(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).out.aggregate('x').out.where(without('x'))", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_withSideEffectXa_graph_verticesX2XX_VX1X_out_whereXneqXaXX(
                final Object v1Id, final Object v2Id) {
            TraversalScriptHelper.compute("g.withSideEffect('a'){graph.vertices(v2Id).next()}.V(v1Id).out.where(neq('a'))", g, "graph", graph, "v1Id", v1Id, "v2Id", v2Id)
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_repeatXbothEXcreatedX_whereXwithoutXeXX_aggregateXeX_otherVX_emit_path(
                final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).repeat(__.bothE('created').where(without('e')).aggregate('e').otherV).emit.path", g, "v1Id", v1Id)
        }

        // hasNot functionality

        @Override
        public Traversal<Vertex, String> get_g_V_whereXnotXoutXcreatedXXX_name() {
            TraversalScriptHelper.compute("g.V.where(not(out('created'))).name", g);
        }

        // complex and/or functionality

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_select_byXnameX() {
            TraversalScriptHelper.compute("g.V.as('a').out.as('b').where(and(__.as('a').out('knows').as('b'),or(__.as('b').out('created').has('name','ripple'),__.as('b').in('knows').count.is(not(eq(0)))))).select.by('name')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_outXcreatedX_asXbX_in_asXcX_whereXa__eqXcX_andXasXaX_outXknowsXXX_select_byXnameX() {
            TraversalScriptHelper.compute("g.V.as('a').out('created').as('b').in.as('c').where('a',eq('c').and(__.as('a').out('knows'))).select.by('name')", g)
        }

        /*@Override
        public Traversal<Vertex, String> get_g_V_asXaX_outXknowsX_asXbX_whereXasXa__bX_outXcreatedX_hasXname__rippleX_name() {
            TraversalScriptHelper.compute("g.V.as('a').out('knows').as('b').where(__.as('a', 'b').out('created').has('name', 'ripple')).name", g)
        }*/
    }
}
