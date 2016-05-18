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
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalScriptHelper
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyVertexTest {

    public static class Traversals extends VertexTest {

        @Override
        public Traversal<Vertex, String> get_g_VXlistXv1_v2_v3XX_name() {
            TraversalScriptHelper.compute("g.V(ids).name", g, "ids", [convertToVertex(graph, "marko"), convertToVertex(graph, "vadas"), convertToVertex(graph, "lop")])
        }

        @Override
        public Traversal<Vertex, String> get_g_VXlistX1_2_3XX_name() {
            TraversalScriptHelper.compute("g.V(ids).name", g, "ids", [convertToVertexId(graph, "marko"), convertToVertexId(graph, "vadas"), convertToVertexId(graph, "lop")])
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V() {
            TraversalScriptHelper.compute("g.V", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).out", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_in(final Object v2Id) {
            TraversalScriptHelper.compute("g.V(v2Id).in", g, "v2Id", v2Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_both(final Object v4Id) {
            TraversalScriptHelper.compute("g.V(v4Id).both", g, "v4Id", v4Id);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E() {
            TraversalScriptHelper.compute("g.E", g);
        }

        @Override
        public Traversal<Edge, Edge> get_g_EX11X(final Object e11Id) {
            TraversalScriptHelper.compute("g.E(e11Id)", g, "e11Id", e11Id)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outE(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).outE", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX2X_inE(final Object v2Id) {
            TraversalScriptHelper.compute("g.V(v2Id).inE", g, "v2Id", v2Id);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_bothE(final Object v4Id) {
            TraversalScriptHelper.compute("g.V(v4Id).bothE", g, "v4Id", v4Id);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_bothEXcreatedX(final Object v4Id) {
            TraversalScriptHelper.compute("g.V(v4Id).bothE('created')", g, "v4Id", v4Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_inV(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).outE.inV", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_inE_outV(final Object v2Id) {
            TraversalScriptHelper.compute("g.V(v2Id).inE.outV", g, "v2Id", v2Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_outE_hasXweight_1X_outV() {
            TraversalScriptHelper.compute("g.V.outE.has('weight', 1.0d).outV", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_outE_inV_inE_inV_both_name() {
            TraversalScriptHelper.compute("g.V.out.outE.inV.inE.inV.both.name", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_outEXknowsX_bothV_name(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).outE('knows').bothV.name", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).out('knows')", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknows_createdX(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).out('knows', 'created')", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outEXknowsX_inV(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).outE('knows').inV()", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outEXknows_createdX_inV(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).outE('knows', 'created').inV", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_out() {
            TraversalScriptHelper.compute("g.V.out().out()", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_out_out(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).out.out.out", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_name(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).out.name", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_otherV(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).outE.otherV", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_bothE_otherV(final Object v4Id) {
            TraversalScriptHelper.compute("g.V(v4Id).bothE.otherV", g, "v4Id", v4Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_bothE_hasXweight_lt_1X_otherV(final Object v4Id) {
            TraversalScriptHelper.compute("g.V(v4Id).bothE.has('weight', lt(1.0d)).otherV", g, "v4Id", v4Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_to_XOUT_knowsX(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).to(Direction.OUT, 'knows')", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1_2_3_4X_name(
                final Object v1Id, final Object v2Id, final Object v3Id, final Object v4Id) {
            g.V(v3Id).drop().iterate();
            TraversalScriptHelper.compute("g.V(v1Id, v2Id, v4Id, v3Id).name", g, "v1Id", v1Id, "v2Id", v2Id, "v3Id", v3Id, "v4Id", v4Id);
        }
    }
}
