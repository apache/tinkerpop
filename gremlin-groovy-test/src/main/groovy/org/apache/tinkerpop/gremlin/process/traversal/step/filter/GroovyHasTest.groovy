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

import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex

import static org.apache.tinkerpop.gremlin.structure.P.gt
import static org.apache.tinkerpop.gremlin.structure.P.inside

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyHasTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends HasTest {

        @Override
        public Traversal<Edge, Edge> get_g_EX11X_outV_outE_hasXid_10X(final Object e11Id, final Object e8Id) {
            g.E(e11Id).outV.outE.has(T.id, e8Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_outXknowsX_hasXoutXcreatedXX_name() {
            g.V.out('knows').has(__.out('created')).name
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXkeyX(final Object v1Id, final String key) {
            g.V(v1Id).has(key)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXname_markoX(final Object v1Id) {
            g.V(v1Id).has('name', 'marko')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX() {
            g.V.has('name', 'marko')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_blahX() {
            g.V.has('name', 'blah')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXblahX() {
            g.V.has('blah')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXage_gt_30X(final Object v1Id) {
            g.V(v1Id).has('age', gt(30))
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2X(final Object v1Id, final Object v2Id) {
            g.V(v1Id).out.hasId(v2Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X() {
            g.V.has('age', gt(30))
        }

        @Override
        public Traversal<Edge, Edge> get_g_EX7X_hasLabelXknowsX(final Object e7Id) {
            g.E(e7Id).hasLabel('knows')
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXknowsX() {
            g.E.hasLabel('knows')
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXuses_traversesX() {
            g.E.hasLabel('uses', 'traverses')
        }

        @Override
        Traversal<Vertex, Vertex> get_g_V_hasLabelXperson_software_blahX() {
            g.V.hasLabel("person", "software", 'blah');
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_hasXperson_name_markoX_age() {
            g.V.has('person', 'name', 'marko').age;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_hasXweight_inside_0_06X_inV(final Object v1Id) {
            g.V(v1Id).outE.has('weight', inside(0.0d, 0.6d)).inV
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends HasTest {
        @Override
        public Traversal<Edge, Edge> get_g_EX11X_outV_outE_hasXid_10X(final Object e11Id, final Object e8Id) {
            ComputerTestHelper.compute("g.E(e11Id).outV.outE.has(T.id, e8Id)", g, "e11Id", e11Id, "e8Id", e8Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_outXknowsX_hasXoutXcreatedXX_name() {
            ComputerTestHelper.compute("g.V.out('knows').has(__.out('created')).name", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXkeyX(final Object v1Id, final String key) {
            ComputerTestHelper.compute("g.V(v1Id).has('${key}')", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXname_markoX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(v1Id).has('name', 'marko')", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX() {
            ComputerTestHelper.compute("g.V.has('name', 'marko')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_blahX() {
            ComputerTestHelper.compute(" g.V.has('name', 'blah')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXblahX() {
            ComputerTestHelper.compute("g.V.has('blah')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXage_gt_30X(final Object v1Id) {
            ComputerTestHelper.compute("g.V(v1Id).has('age', gt(30))", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2X(final Object v1Id, final Object v2Id) {
            ComputerTestHelper.compute(" g.V(v1Id).out.hasId(v2Id)", g, "v1Id", v1Id, "v2Id", v2Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X() {
            ComputerTestHelper.compute("g.V.has('age', gt(30))", g);
        }

        @Override
        public Traversal<Edge, Edge> get_g_EX7X_hasLabelXknowsX(final Object e7Id) {
            ComputerTestHelper.compute("g.E(e7Id).hasLabel('knows')", g, "e7Id", e7Id);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXknowsX() {
            ComputerTestHelper.compute("g.E.hasLabel('knows')", g);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXuses_traversesX() {
            ComputerTestHelper.compute("g.E.hasLabel('uses', 'traverses')", g);
        }

        @Override
        Traversal<Vertex, Vertex> get_g_V_hasLabelXperson_software_blahX() {
            ComputerTestHelper.compute("g.V.hasLabel('person', 'software', 'blah')", g);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_hasXperson_name_markoX_age() {
            ComputerTestHelper.compute("g.V.has('person', 'name', 'marko').age", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_hasXweight_inside_0_06X_inV(final Object v1Id) {
            ComputerTestHelper.compute("g.V(v1Id).outE.has('weight', inside(0.0d, 0.6d)).inV", g, "v1Id", v1Id);
        }
    }
}
