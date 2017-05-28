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
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyHasTest {

    public static class Traversals extends HasTest {
        @Override
        public Traversal<Edge, Edge> get_g_EX11X_outV_outE_hasXid_10X(final Object e11Id, final Object e8Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.E(e11Id).outV.outE.has(T.id, e8Id)", "e11Id", e11Id, "e8Id", e8Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_outXcreatedX_hasXname__mapXlengthX_isXgtX3XXX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out('created').has('name',map{it.length()}.is(gt(3))).name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXkeyX(final Object v1Id, final String key) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).has(k)", "v1Id", v1Id, "k", key);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXname_markoX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).has('name', 'marko')", "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('name', 'marko')");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_blahX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('name', 'blah')");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXblahX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('blah')");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXage_gt_30X(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).has('age',gt(30))", "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VXv1X_hasXage_gt_30X(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(g.V(v1Id).next()).has('age',gt(30))", "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2X(final Object v1Id, final Object v2Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out.hasId(v2Id)", "v1Id", v1Id, "v2Id", v2Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2_3X(
                final Object v1Id, final Object v2Id, final Object v3Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out.hasId(v2Id, v3Id)", "v1Id", v1Id, "v2Id", v2Id, "v3Id", v3Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_hasXid_lt_3X(final Object v1Id, final Object v3Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out().has(T.id, P.lt(v3Id))", "v1Id", v1Id, "v3Id", v3Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('age',gt(30))");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXage_isXgt_30XX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('age', __.is(gt(30)))");
        }

        @Override
        public Traversal<Edge, Edge> get_g_EX7X_hasLabelXknowsX(final Object e7Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.E(e7Id).hasLabel('knows')", "e7Id", e7Id);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXknowsX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.E.hasLabel('knows')");
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXuses_traversesX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.E.hasLabel('uses', 'traverses')");
        }

        @Override
        Traversal<Vertex, Vertex> get_g_V_hasLabelXperson_software_blahX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('person', 'software', 'blah')");
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_hasXperson_name_markoX_age() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('person', 'name', 'marko').age");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_hasXweight_inside_0_06X_inV(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).outE.has('weight', inside(0.0d, 0.6d)).inV", "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXlocationX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('location')")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_in_hasIdXneqX1XX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.in.hasId(neq(v1Id))", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasLabelXpersonX_hasXage_notXlteX10X_andXnotXbetweenX11_20XXXX_andXltX29X_orXeqX35XXXX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('person').has('age', P.not(lte(10).and(P.not(between(11,20)))).and(lt(29).or(eq(35)))).name")
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_both_properties_dedup_hasKeyXageX_value() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.both.properties().dedup.hasKey('age').value")
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_both_properties_dedup_hasKeyXageX_hasValueXgtX30XX_value() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.both.properties().dedup.hasKey('age').hasValue(gt(30)).value")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasNotXageX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasNot('age').name");
        }
    }
}
