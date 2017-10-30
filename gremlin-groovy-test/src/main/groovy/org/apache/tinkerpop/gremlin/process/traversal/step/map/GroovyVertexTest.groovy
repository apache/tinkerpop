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
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyVertexTest {

    public static class Traversals extends VertexTest {

        @Override
        public Traversal<Vertex, String> get_g_VXlistXv1_v2_v3XX_name(
                final Vertex v1, final Vertex v2, final Vertex v3) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(ids).name", "ids", [v1, v2, v3])
        }

        @Override
        public Traversal<Vertex, String> get_g_VXlistX1_2_3XX_name(
                final Object v1Id, final Object v2Id, final Object v3Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(ids).name", "ids", [v1Id, v2Id, v3Id])
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out", "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_in(final Object v2Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v2Id).in", "v2Id", v2Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_both(final Object v4Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v4Id).both", "v4Id", v4Id);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.E")
        }

        @Override
        public Traversal<Edge, Edge> get_g_EX11X(final Object e11Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.E(e11Id)", "e11Id", e11Id)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outE(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).outE", "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX2X_inE(final Object v2Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v2Id).inE", "v2Id", v2Id);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_bothE(final Object v4Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v4Id).bothE", "v4Id", v4Id);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_bothEXcreatedX(final Object v4Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v4Id).bothE('created')", "v4Id", v4Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_inV(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).outE.inV", "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_inE_outV(final Object v2Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v2Id).inE.outV", "v2Id", v2Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_outE_hasXweight_1X_outV() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.outE.has('weight', 1.0d).outV")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_outE_inV_inE_inV_both_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out.outE.inV.inE.inV.both.name")
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_outEXknowsX_bothV_name(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).outE('knows').bothV.name", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out('knows')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknows_createdX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out('knows', 'created')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outEXknowsX_inV(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).outE('knows').inV()", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outEXknows_createdX_inV(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).outE('knows', 'created').inV", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_out() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out().out()")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_out_out(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out.out.out", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_name(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).out.name", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_otherV(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).outE.otherV", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_bothE_otherV(final Object v4Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v4Id).bothE.otherV", "v4Id", v4Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_bothE_hasXweight_lt_1X_otherV(final Object v4Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v4Id).bothE.has('weight', lt(1.0d)).otherV", "v4Id", v4Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_to_XOUT_knowsX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).to(Direction.OUT, 'knows')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1_2_3_4X_name(
                final Object v1Id, final Object v2Id, final Object v3Id, final Object v4Id) {
            g.V(v3Id).drop().iterate();
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id, v2Id, v4Id, v3Id).name", "v1Id", v1Id, "v2Id", v2Id, "v3Id", v3Id, "v4Id", v4Id)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasLabelXpersonX_V_hasLabelXsoftwareX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('person').V.hasLabel('software').name")
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_bothEXselfX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().bothE('self')")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_bothXselfX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().both('self')")
        }
    }
}
