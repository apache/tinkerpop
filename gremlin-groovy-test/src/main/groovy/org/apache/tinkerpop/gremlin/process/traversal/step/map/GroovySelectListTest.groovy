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

import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Order
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.junit.Test

import static __.values

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySelectListTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends SelectListTest {

        @Override
        public Traversal<Vertex, Map<String, List<Vertex>>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectList(final Object v1Id) {
            g.V(v1Id).as('a').out('knows').as('b').selectList()
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectList_byXnameX(
                final Object v1Id) {
            g.V(v1Id).as('a').out('knows').as('b').selectList.by('name')
        }

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectListXaX(final Object v1Id) {
            g.V(v1Id).as('a').out('knows').as('b').selectList('a')
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectListXaX_byXnameX(
                final Object v1Id) {
            g.V(v1Id).as('a').out('knows').as('b').selectList('a').by('name')
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_V_asXaX_out_asXbX_selectList_byXnameX() {
            g.V.as('a').out.as('b').selectList.by('name')
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_V_asXaX_out_aggregateXxX_asXbX_selectList_byXnameX() {
            g.V.as('a').out.aggregate('x').as('b').selectList.by('name')
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_V_asXaX_name_order_asXbX_selectList_byXnameX_by_XitX() {
            g.V().as('a').name.order().as('b').selectList.by('name').by
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_selectList_byXskillX_byXnameX() {
            g.V.has('name', 'gremlin').inE('uses').order.by('skill', Order.incr).as('a').outV.as('b').selectList.by('skill').by('name')
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasXname_isXmarkoXX_asXaX_selectList() {
            return g.V.has(values('name').is('marko')).as('a').selectList
        }

        @Override
        public Traversal<Vertex, Map<String, List<Map<String, Long>>>> get_g_V_label_groupCount_asXxX_selectList() {
            return g.V().label().groupCount().as('x').selectList()
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasLabelXpersonX_asXpersonX_localXbothE_label_groupCountX_asXrelationsX_selectList_byXnameX_by() {
            return g.V().hasLabel('person').as('person').local(__.bothE().label().groupCount()).as('relations').selectList().by('name').by()
        }

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_V_asXaX_out_asXaX_out_asXaX_selectListXaX() {
            return g.V().as("a").out().as("a").out().as("a").selectList("a");
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_asXaX_out_asXaX_out_asXaX_selectListXaX_byXnameX() {
            return g.V().as("a").out().as("a").out().as("a").<String>selectList("a").by("name");
        }

        //

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_VX1X_asXhereX_out_selectListXhereX(final Object v1Id) {
            g.V(v1Id).as('here').out.selectList('here')
        }

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectListXhereX(final Object v4Id) {
            g.V(v4Id).out.as('here').has('lang', 'java').selectList('here')
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectListXhereX_unfold_name(
                final Object v4Id) {
            g.V(v4Id).out.as('here').has('lang', 'java').selectList('here').unfold().name
        }

        @Override
        public Traversal<Vertex, List<Edge>> get_g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectListXhereX(final Object v1Id) {
            g.V(v1Id).outE.as('here').inV.has('name', 'vadas').selectList('here')
        }

        @Override
        public Traversal<Vertex, List<Edge>> get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectListXhereX(
                final Object v1Id) {
            g.V(v1Id).outE('knows').has('weight', 1.0d).as('here').inV.has('name', 'josh').selectList('here')
        }

        @Override
        public Traversal<Vertex, List<Edge>> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectListXhereX(
                final Object v1Id) {
            g.V(v1Id).outE('knows').as('here').has('weight', 1.0d).inV.has('name', 'josh').selectList('here')
        }

        @Override
        public Traversal<Vertex, List<Edge>> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectListXhereX(
                final Object v1Id) {
            g.V(v1Id).outE("knows").as('here').has('weight', 1.0d).as('fake').inV.has("name", 'josh').selectList('here')
        }

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_V_asXhereXout_name_selectListXhereX() {
            g.V().as("here").out.name.selectList("here");
        }


        @Override
        public Traversal<Vertex, Map<List<String>, Long>> get_g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectListXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectListXprojectXX_groupCount_byXnameX() {
            g.V.out('created')
                    .union(__.as('project').in('created').has('name', 'marko').selectList('project'),
                    __.as('project').in('created').in('knows').has('name', 'marko').selectList('project')).groupCount().by('name');
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends SelectListTest {

        @Override
        public Traversal<Vertex, Map<String, List<Vertex>>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectList(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).as('a').out('knows').as('b').selectList()", g);
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_VX1X_asXaX_outXknowsX_asXbX_selectList_byXnameX() {
        }

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectListXaX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).as('a').out('knows').as('b').selectList('a')", g);
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_VX1X_asXaX_outXknowsX_asXbX_selectListXaX_byXnameX() {
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_asXaX_out_asXbX_selectList_byXnameX() {
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_asXaX_name_order_asXbX_selectList_byXnameX_byXitX() {
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_selectList_byXskillX_byXnameX() {
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasXname_isXmarkoXX_asXaX_selectList() {
            ComputerTestHelper.compute("g.V.has(values('name').is('marko')).as('a').selectList", g)
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_label_groupCount_asXxX_selectList() {
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        void g_V_hasLabelXpersonX_asXpersonX_localXbothE_label_groupCountX_asXrelationsX_selectList_byXnameX_by() {
        }

        @Override
        Traversal<Vertex, Map<String, List<String>>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectList_byXnameX(Object v1Id) {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        Traversal<Vertex, List<String>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectListXaX_byXnameX(Object v1Id) {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        Traversal<Vertex, Map<String, List<String>>> get_g_V_asXaX_out_asXbX_selectList_byXnameX() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        Traversal<Vertex, Map<String, List<String>>> get_g_V_asXaX_out_aggregateXxX_asXbX_selectList_byXnameX() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        Traversal<Vertex, Map<String, List<String>>> get_g_V_asXaX_name_order_asXbX_selectList_byXnameX_by_XitX() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_selectList_byXskillX_byXnameX() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        Traversal<Vertex, Map<String, List<Map<String, Long>>>> get_g_V_label_groupCount_asXxX_selectList() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasLabelXpersonX_asXpersonX_localXbothE_label_groupCountX_asXrelationsX_selectList_byXnameX_by() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_V_asXaX_out_asXaX_out_asXaX_selectListXaX() {
            ComputerTestHelper.compute("g.V().as('a').out().as('a').out().as('a').selectList('a')", g);
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_asXaX_out_asXaX_out_asXaX_selectListXaX_byXnameX() {
            ComputerTestHelper.compute("g.V().as('a').out().as('a').out().as('a').selectList('a').by('name')", g);
        }

        //

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_VX1X_asXhereX_out_selectListXhereX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).as('here').out.selectList('here')", g);
        }

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectListXhereX(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).out.as('here').has('lang', 'java').selectList('here')", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectListXhereX_unfold_name(
                final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).out.as('here').has('lang', 'java').selectList('here').unfold().name", g);
        }

        @Override
        public Traversal<Vertex, List<Edge>> get_g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectListXhereX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE.as('here').inV.has('name', 'vadas').selectList('here')", g);
        }

        @Override
        public Traversal<Vertex, List<Edge>> get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectListXhereX(
                final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE('knows').has('weight', 1.0d).as('here').inV.has('name', 'josh').selectList('here')", g);
        }

        @Override
        public Traversal<Vertex, List<Edge>> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectListXhereX(
                final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE('knows').as('here').has('weight', 1.0d).inV.has('name','josh').selectList('here')", g);
        }

        @Override
        public Traversal<Vertex, List<Edge>> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectListXhereX(
                final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE('knows').as('here').has('weight', 1.0d).as('fake').inV.has('name','josh').selectList('here')", g);
        }

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_V_asXhereXout_name_selectListXhereX() {
            ComputerTestHelper.compute("g.V().as('here').out.name.selectList('here')", g);
        }

        @Override
        public Traversal<Vertex, Map<List<String>, Long>> get_g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectListXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectListXprojectXX_groupCount_byXnameX() {
            ComputerTestHelper.compute("""
            g.V.out('created')
                    .union(__.as('project').in('created').has('name', 'marko').selectList('project'),
                    __.as('project').in('created').in('knows').has('name', 'marko').selectList('project')).groupCount().by('name');
            """, g)
        }
    }
}
