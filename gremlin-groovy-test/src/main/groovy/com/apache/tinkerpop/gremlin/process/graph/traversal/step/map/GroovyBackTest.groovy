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
package com.apache.tinkerpop.gremlin.process.graph.traversal.step.map

import com.apache.tinkerpop.gremlin.process.Traversal
import com.apache.tinkerpop.gremlin.process.graph.traversal.__
import com.apache.tinkerpop.gremlin.process.ComputerTestHelper
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.map.BackTest
import com.apache.tinkerpop.gremlin.structure.Edge
import com.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyBackTest {

    public static class StandardTest extends BackTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXhereX_out_backXhereX(final Object v1Id) {
            g.V(v1Id).as('here').out.back('here')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_out_asXhereX_hasXlang_javaX_backXhereX(final Object v4Id) {
            g.V(v4Id).out.as('here').has('lang', 'java').back('here')
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_out_asXhereX_hasXlang_javaX_backXhereX_name(
                final Object v4Id) {
            g.V(v4Id).out.as('here').has('lang', 'java').back('here').name
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outE_asXhereX_inV_hasXname_vadasX_backXhereX(final Object v1Id) {
            g.V(v1Id).outE.as('here').inV.has('name', 'vadas').back('here')
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX(
                final Object v1Id) {
            g.V(v1Id).outE('knows').has('weight', 1.0d).as('here').inV.has('name', 'josh').back('here')
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_backXhereX(
                final Object v1Id) {
            g.V(v1Id).outE('knows').as('here').has('weight', 1.0d).inV.has('name', 'josh').back('here')
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_backXhereX(
                final Object v1Id) {
            g.V(v1Id).outE("knows").as('here').has('weight', 1.0d).as('fake').inV.has("name", 'josh').back('here')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXhereXout_name_backXhereX() {
            g.V().as("here").out.name.back("here");
        }


        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_backXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_backXprojectXX_groupCount_byXnameX() {
            g.V.out('created')
                    .union(__.as('project').in('created').has('name', 'marko').back('project'),
                    __.as('project').in('created').in('knows').has('name', 'marko').back('project')).groupCount().by('name');
        }
    }

    public static class ComputerTest extends BackTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXhereX_out_backXhereX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).as('here').out.back('here')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_out_asXhereX_hasXlang_javaX_backXhereX(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).out.as('here').has('lang', 'java').back('here')", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_out_asXhereX_hasXlang_javaX_backXhereX_name(
                final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).out.as('here').has('lang', 'java').back('here').name", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outE_asXhereX_inV_hasXname_vadasX_backXhereX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE.as('here').inV.has('name', 'vadas').back('here')", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX(
                final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE('knows').has('weight', 1.0d).as('here').inV.has('name', 'josh').back('here')", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_backXhereX(
                final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE('knows').as('here').has('weight', 1.0d).inV.has('name','josh').back('here')", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_backXhereX(
                final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE('knows').as('here').has('weight', 1.0d).as('fake').inV.has('name','josh').back('here')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXhereXout_name_backXhereX() {
            ComputerTestHelper.compute("g.V().as('here').out.name.back('here')", g);
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_backXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_backXprojectXX_groupCount_byXnameX() {
            ComputerTestHelper.compute("""
            g.V.out('created')
                    .union(__.as('project').in('created').has('name', 'marko').back('project'),
                    __.as('project').in('created').in('knows').has('name', 'marko').back('project')).groupCount().by('name');
            """, g)
        }
    }
}
