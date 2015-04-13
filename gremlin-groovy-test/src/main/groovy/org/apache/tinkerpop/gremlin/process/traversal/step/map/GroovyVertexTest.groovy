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
import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.structure.Compare
import org.apache.tinkerpop.gremlin.structure.Direction
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyVertexTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends VertexTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V() {
            g.V
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out(final Object v1Id) {
            g.V(v1Id).out
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_in(final Object v2Id) {
            g.V(v2Id).in
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_both(final Object v4Id) {
            g.V(v4Id).both
        }


        @Override
        public Traversal<Edge, Edge> get_g_E() {
            g.E
        }

        @Override
        public Traversal<Edge, Edge> get_g_EX11X(final Object e11Id) {
            g.E(e11Id);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outE(final Object v1Id) {
            g.V(v1Id).outE
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX2X_inE(final Object v2Id) {
            g.V(v2Id).inE
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_bothE(final Object v4Id) {
            g.V(v4Id).bothE
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_bothEXcreatedX(final Object v4Id) {
            g.V(v4Id).bothE('created')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_inV(final Object v1Id) {
            g.V(v1Id).outE.inV
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_inE_outV(final Object v2Id) {
            g.V(v2Id).inE.outV
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_outE_hasXweight_1X_outV() {
            g.V.outE.has('weight', 1.0d).outV
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_outE_inV_inE_inV_both_name() {
            g.V.out.outE.inV.inE.inV.both.name
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_outEXknowsX_bothV_name(final Object v1Id) {
            g.V(v1Id).outE('knows').bothV.name
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX(final Object v1Id) {
            g.V(v1Id).out('knows')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknows_createdX(final Object v1Id) {
            g.V(v1Id).out('knows', 'created')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outEXknowsX_inV(final Object v1Id) {
            g.V(v1Id).outE('knows').inV()
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outEXknows_createdX_inV(final Object v1Id) {
            g.V(v1Id).outE('knows', 'created').inV
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_out() {
            g.V().out().out()
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_out_out(final Object v1Id) {
            g.V(v1Id).out.out.out
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_name(final Object v1Id) {
            g.V(v1Id).out.name
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_otherV(final Object v1Id) {
            g.V(v1Id).outE.otherV
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_bothE_otherV(final Object v4Id) {
            g.V(v4Id).bothE.otherV
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_bothE_hasXweight_lt_1X_otherV(final Object v4Id) {
            g.V(v4Id).bothE.has('weight', Compare.lt, 1.0d).otherV
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_to_XOUT_knowsX(final Object v1Id) {
            g.V(v1Id).to(Direction.OUT, 'knows');
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends VertexTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V() {
            ComputerTestHelper.compute("g.V", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_in(final Object v2Id) {
            ComputerTestHelper.compute("g.V(${v2Id}).in", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_both(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).both", g);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E() {
            ComputerTestHelper.compute("g.E", g);
        }

        @Override
        public Traversal<Edge, Edge> get_g_EX11X(final Object e11Id) {
            ComputerTestHelper.compute("g.E($e11Id)", g)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outE(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX2X_inE(final Object v2Id) {
            ComputerTestHelper.compute("g.V(${v2Id}).inE", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_bothE(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).bothE", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX4X_bothEXcreatedX(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).bothE('created')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_inV(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE.inV", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_inE_outV(final Object v2Id) {
            ComputerTestHelper.compute("g.V(${v2Id}).inE.outV", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_outE_hasXweight_1X_outV() {
            ComputerTestHelper.compute("g.V.outE.has('weight', 1.0d).outV", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_outE_inV_inE_inV_both_name() {
            ComputerTestHelper.compute("g.V.out.outE.inV.inE.inV.both.name", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_outEXknowsX_bothV_name(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE('knows').bothV.name", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out('knows')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknows_createdX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out('knows', 'created')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outEXknowsX_inV(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE('knows').inV()", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outEXknows_createdX_inV(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE('knows', 'created').inV", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_out() {
            ComputerTestHelper.compute("g.V().out().out()", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_out_out(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out.out.out", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_name(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out.name", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_otherV(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE.otherV", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_bothE_otherV(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).bothE.otherV", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_bothE_hasXweight_lt_1X_otherV(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).bothE.has('weight', Compare.lt, 1.0d).otherV", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_to_XOUT_knowsX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).to(Direction.OUT, 'knows')", g);
        }
    }
}
