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
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyRangeTest {

    public static class Traversals extends RangeTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_limitX2X(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out.limit(2)", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_localXoutE_limitX1X_inVX_limitX3X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.local(__.outE.limit(3)).inV.limit(3)")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out('knows').outE('created')[0].inV()", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out('knows').out('created')[0]", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out('created').in('created')[1..3]", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out('created').inE('created')[1..3].outV", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_rangeX5_11X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().repeat(__.both).times(3)[5..11]")
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_asXaX_in_asXaX_in_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_limitXlocal_2X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().as('a').in().as('a').in().as('a').select('a').by(unfold().values('name').fold).limit(local,2)")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_in_asXaX_in_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_limitXlocal_1X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().as('a').in().as('a').in().as('a').select('a').by(unfold().values('name').fold).limit(local,1)")
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_3X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().as('a').out().as('a').out().as('a').select('a').by(unfold().values('name').fold).range(local,1,3)")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_2X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().as('a').out().as('a').out().as('a').select('a').by(unfold().values('name').fold).range(local,1,2)")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_rangeXlocal_4_5X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().as('a').out().as('a').out().as('a').select('a').by(unfold().values('name').fold).range(local,4,5)")
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_2X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').in.as('b').in.as('c').select('a','b','c').by('name').limit(local,2)")
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_1X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').in.as('b').in.as('c').select('a','b','c').by('name').limit(local,1)")
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_3X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').out.as('b').out.as('c').select('a','b','c').by('name').range(local,1,3)")
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_2X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').out.as('b').out.as('c').select('a','b','c').by('name').range(local,1,2)")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_limitXlocal_3X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.name.limit(local, 3)")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_rangeXlocal_1_4X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.name.range(local, 1, 4)")
        }
    }
}
