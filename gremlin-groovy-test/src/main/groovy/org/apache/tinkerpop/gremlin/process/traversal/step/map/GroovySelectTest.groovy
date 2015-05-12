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
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class GroovySelectTest {

    public static class Traversals extends SelectTest {

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_VX1X_asXaX_outXknowsX_asXbX_select(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).as('a').out('knows').as('b').select()", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_VX1X_asXaX_outXknowsX_asXbX_select_byXnameX(
                final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).as('a').out('knows').as('b').select.by('name')", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).as('a').out('knows').as('b').select('a')", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX(
                final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).as('a').out('knows').as('b').select('a').by('name')", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_select_byXnameX() {
            TraversalScriptHelper.compute("g.V.as('a').out.as('b').select.by('name')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_aggregateXxX_asXbX_select_byXnameX() {
            TraversalScriptHelper.compute("g.V.as('a').out.aggregate('x').as('b').select.by('name')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_name_order_asXbX_select_byXnameX_by_XitX() {
            TraversalScriptHelper.compute("g.V().as('a').name.order().as('b').select.by('name').by", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_select_byXskillX_byXnameX() {
            TraversalScriptHelper.compute("g.V.has('name', 'gremlin').inE('uses').order.by('skill', Order.incr).as('a').outV.as('b').select.by('skill').by('name')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXname_isXmarkoXX_asXaX_select() {
            TraversalScriptHelper.compute("g.V.has(values('name').is('marko')).as('a').select", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_label_groupCount_asXxX_select() {
            TraversalScriptHelper.compute("g.V().label().groupCount().as('x').select()", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXpersonX_asXpersonX_localXbothE_label_groupCountX_asXrelationsX_select_byXnameX_by() {
            TraversalScriptHelper.compute("g.V().hasLabel('person').as('person').local(__.bothE().label().groupCount()).as('relations').select().by('name').by()", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_select() {
            TraversalScriptHelper.compute("g.V().choose(__.outE().count().is(0L), __.as('a'), __.as('b')).select()", g)
        }

        @Override
        Traversal<Vertex, Map<String, List<Vertex>>> get_g_V_asXaX_outXcreatedX_asXaX_select() {
            TraversalScriptHelper.compute("g.V.as('a').out('created').as('a').select", g)
        }
//

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXhereX_out_selectXhereX(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).as('here').out.select('here')", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX(final Object v4Id) {
            TraversalScriptHelper.compute("g.V(v4Id).out.as('here').has('lang', 'java').select('here')", g, "v4Id", v4Id)
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX_name(
                final Object v4Id) {
            TraversalScriptHelper.compute("g.V(v4Id).out.as('here').has('lang', 'java').select('here').name", g, "v4Id", v4Id)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX(final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).outE.as('here').inV.has('name', 'vadas').select('here')", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX(
                final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).outE('knows').has('weight', 1.0d).as('here').inV.has('name', 'josh').select('here')", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectXhereX(
                final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).outE('knows').as('here').has('weight', 1.0d).inV.has('name', 'josh').select('here')", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectXhereX(
                final Object v1Id) {
            TraversalScriptHelper.compute("g.V(v1Id).outE('knows').as('here').has('weight', 1.0d).as('fake').inV.has('name', 'josh').select('here')", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXhereXout_name_selectXhereX() {
            TraversalScriptHelper.compute("g.V().as('here').out.name.select('here')", g)
        }


        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectXprojectXX_groupCount_byXnameX() {
            TraversalScriptHelper.compute("""g.V.out('created')
                    .union(__.as('project').in('created').has('name', 'marko').select('project'),
                    __.as('project').in('created').in('knows').has('name', 'marko').select('project')).groupCount().by('name')""", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_hasXname_markoX_asXbX_asXcX_select_by_byXnameX_byXageX() {
            TraversalScriptHelper.compute("g.V.as('a').has('name', 'marko').as('b').as('c').select().by().by('name').by('age')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_select_byXnameX_byXlangX_byXinXcreatedX_valuesXnameX_fold_orderXlocalXX() {
            TraversalScriptHelper.compute("""g.V.hasLabel('software').as('name').as('language').as('creators').select().by('name').by('lang').
                    by(__.in('created').values('name').fold().order(local))""", g)
        }
    }
}
