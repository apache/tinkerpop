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
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyWhereTest {

    public static class Traversals extends WhereTest {

        /// where(local)

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_eqXbXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('age').as('a').out.in.has('age').as('b').select('a','b').where('a', eq('b'))")
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_neqXbXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('age').as('a').out.in.has('age').as('b').select('a','b').where('a', neq('b'))")
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('age').as('a').out.in.has('age').as('b').select('a','b').where(__.as('b').has('name', 'marko'))")
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().has('age').as('a').out.in.has('age').as('b').select('a','b').where(__.as('a').out('knows').as('b'))")
        }

        /// where(global)

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_neqXbXX_name(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).as('a').out('created').in('created').as('b').where('a', neq('b')).name", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Object> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).as('a').out('created').in('created').as('b').where(__.as('b').out('created').has('name','ripple')).values('age','name')", "v1Id", v1Id)
        }

        // except/retain functionality

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXeqXaXX_name(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).as('a').out('created').in('created').where(eq('a')).name", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_name(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).as('a').out('created').in('created').where(neq('a')).name", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_aggregateXxX_out_whereXnotXwithinXaXXX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out.aggregate('x').out.where(P.not(within('x')))", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_withSideEffectXa_graph_verticesX2XX_VX1X_out_whereXneqXaXX(
                final Object v1Id, final Object v2Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.withSideEffect('a'){g.V(v2Id).next()}.V(v1Id).out.where(neq('a'))", "graph", graph, "v1Id", v1Id, "v2Id", v2Id)
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_repeatXbothEXcreatedX_whereXwithoutXeXX_aggregateXeX_otherVX_emit_path(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).repeat(__.bothE('created').where(without('e')).aggregate('e').otherV).emit.path", "v1Id", v1Id)
        }

        // hasNot functionality

        @Override
        public Traversal<Vertex, String> get_g_V_whereXnotXoutXcreatedXXX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.where(__.not(out('created'))).name");
        }

        // complex and/or functionality

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_selectXa_bX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').out.as('b').where(and(__.as('a').out('knows').as('b'),or(__.as('b').out('created').has('name','ripple'),__.as('b').in('knows').count.is(P.not(eq(0)))))).select('a','b')")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_whereXoutXcreatedX_and_outXknowsX_or_inXknowsXX_valuesXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.where(out('created').and.out('knows').or.in('knows')).name")
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_outXcreatedX_asXbX_whereXandXasXbX_in__notXasXaX_outXcreatedX_hasXname_rippleXXX_selectXa_bX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').out('created').as('b').where(and(__.as('b').in,__.not(__.as('a').out('created').has('name','ripple')))).select('a','b')")
        }


        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_bothXknowsX_bothXknowsX_asXdX_whereXc__notXeqXaX_orXeqXdXXXX_selectXa_b_c_dX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').out('created').as('b').in('created').as('c').both('knows').both('knows').as('d').where('c',P.not(eq('a').or(eq('d')))).select('a','b','c','d')")
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_out_asXbX_whereXin_count_isXeqX3XX_or_whereXoutXcreatedX_and_hasXlabel_personXXX_selectXa_bX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').out.as('b').where(__.as('b').in.count.is(eq(3)).or.where(__.as('b').out('created').and.as('b').has(label,'person'))).select('a','b')")
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_gtXbXX_byXageX_selectXa_bX_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').out('created').in('created').as('b').where('a', gt('b')).by('age').select('a', 'b').by('name')")
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_whereXa_gtXbX_orXeqXbXXX_byXageX_byXweightX_byXweightX_selectXa_cX_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').outE('created').as('b').inV().as('c').where('a', gt('b').or(eq('b'))).by('age').by('weight').by('weight').select('a', 'c').by('name')")
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_inXcreatedX_asXdX_whereXa_ltXbX_orXgtXcXX_andXneqXdXXX_byXageX_byXweightX_byXinXcreatedX_valuesXageX_minX_selectXa_c_dX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().as('a').outE('created').as('b').inV().as('c').in('created').as('d').where('a', lt('b').or(gt('c')).and(neq('d'))).by('age').by('weight').by(__.in('created').values('age').min()).select('a', 'c', 'd').by('name')")
        }

    }
}
