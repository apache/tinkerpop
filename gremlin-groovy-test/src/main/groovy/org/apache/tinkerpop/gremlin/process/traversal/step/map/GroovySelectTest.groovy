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

import org.apache.tinkerpop.gremlin.process.traversal.Pop
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class GroovySelectTest {

    public static class Traversals extends SelectTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXaX_repeatXout_asXaXX_timesX2X_selectXfirst_aX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).as('a').repeat(__.out().as('a')).times(2).select(Pop.first, 'a')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXaX_repeatXout_asXaXX_timesX2X_selectXlast_aX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).as('a').repeat(__.out().as('a')).times(2).select(Pop.last, 'a')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).as('a').out('knows').as('b').select('a','b')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX_byXnameX(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).as('a').out('knows').as('b').select('a','b').by('name')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).as('a').out('knows').as('b').select('a')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).as('a').out('knows').as('b').select('a').by('name')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_selectXa_bX_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').out.as('b').select('a','b').by('name')")
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_aggregateXxX_asXbX_selectXa_bX_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').out.aggregate('x').as('b').select('a','b').by('name')")
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_name_order_asXbX_selectXa_bX_byXnameX_by_XitX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().as('a').name.order().as('b').select('a','b').by('name').by")
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_selectXa_bX_byXskillX_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('name', 'gremlin').inE('uses').order.by('skill', Order.incr).as('a').outV.as('b').select('a','b').by('skill').by('name')")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_isXmarkoXX_asXaX_selectXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('name',__.is('marko')).as('a').select('a')")
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_label_groupCount_asXxX_selectXxX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().label().groupCount().as('x').select('x')")
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXpersonX_asXpX_mapXbothE_label_groupCountX_asXrX_selectXp_rX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('person').as('p').map(__.bothE.label.groupCount()).as('r').select('p','r')")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_chooseXselectXaX__selectXaX__selectXbXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.choose(__.outE().count().is(0L), __.as('a'), __.as('b')).choose(select('a'),select('a'),select('b'))")
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_selectXaX_count() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.select('a').count")
        }

        // below are original back()-tests

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_asXhereX_out_selectXhereX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).as('here').out.select('here')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX(final Object v4Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v4Id).out.as('here').has('lang', 'java').select('here')", "v4Id", v4Id)
        }

        @Override
        public Traversal<Vertex, String> get_g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX_name(
                final Object v4Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v4Id).out.as('here').has('lang', 'java').select('here').name", "v4Id", v4Id)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX(final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).outE.as('here').inV.has('name', 'vadas').select('here')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).outE('knows').has('weight', 1.0d).as('here').inV.has('name', 'josh').select('here')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectXhereX(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).outE('knows').as('here').has('weight', 1.0d).inV.has('name', 'josh').select('here')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectXhereX(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).outE('knows').as('here').has('weight', 1.0d).as('fake').inV.has('name', 'josh').select('here')", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXhereXout_name_selectXhereX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().as('here').out.name.select('here')")
        }


        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectXprojectXX_groupCount_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """g.V.out('created')
                    .union(__.as('project').in('created').has('name', 'marko').select('project'),
                    __.as('project').in('created').in('knows').has('name', 'marko').select('project')).groupCount().by('name')""")
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_hasXname_markoX_asXbX_asXcX_selectXa_b_cX_by_byXnameX_byXageX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').has('name', 'marko').as('b').as('c').select('a','b','c').by().by('name').by('age')")
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_selectXname_language_creatorsX_byXnameX_byXlangX_byXinXcreatedX_name_fold_orderXlocalXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """g.V.hasLabel('software').as('name').as('language').as('creators').select('name','language','creators').by('name').by('lang').
                    by(__.in('created').values('name').fold().order(local))""")
        }

        // TINKERPOP-619: select should not throw

        @Override
        public Traversal<Vertex, Object> get_g_V_selectXaX(final Pop pop) {
            final String root = "g.V."
            new ScriptTraversal<>(g, "gremlin-groovy", root + (null == pop ? "select('a')" : "select(${pop}, 'a')"))
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_selectXa_bX(final Pop pop) {
            final String root = "g.V."
            new ScriptTraversal<>(g, "gremlin-groovy", root + (null == pop ? "select('a', 'b')" : "select(${pop}, 'a', 'b')"))
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_valueMap_selectXpop_aX(final Pop pop) {
            final String root = "g.V.valueMap."
            new ScriptTraversal<>(g, "gremlin-groovy", root + (null == pop ? "select('a')" : "select(${pop}, 'a')"))
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_valueMap_selectXpop_a_bX(final Pop pop) {
            final String root = "g.V.valueMap."
            new ScriptTraversal<>(g, "gremlin-groovy", root + (null == pop ? "select('a', 'b')" : "select(${pop}, 'a', 'b')"))
        }

        // when labels don't exist

        @Override
        public Traversal<Vertex, String> get_g_V_untilXout_outX_repeatXin_asXaXX_selectXaX_byXtailXlocalX_nameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.until(__.out.out).repeat(__.in.as('a')).select('a').by(tail(local).name)")
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_untilXout_outX_repeatXin_asXaX_in_asXbXX_selectXa_bX_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.until(__.out.out).repeat(__.in.as('a').in.as('b')).select('a','b').by('name')")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXaX_whereXoutXknowsXX_selectXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().as('a').where(out('knows')).select('a')")
        }

        // select column tests

        @Override
        public Traversal<Vertex, Long> get_g_V_outE_weight_groupCount_selectXvaluesX_unfold() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.outE.weight.groupCount.select(values).unfold")
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_outE_weight_groupCount_unfold_selectXvaluesX_unfold() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.outE.weight.groupCount.unfold.select(values).unfold")
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_outE_weight_groupCount_selectXvaluesX_unfold_groupCount_selectXvaluesX_unfold() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.outE.weight.groupCount.select(values).unfold.groupCount.select(values).unfold")
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_outE_weight_groupCount_selectXkeysX_unfold() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.outE.weight.groupCount.select(keys).unfold")
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_outE_weight_groupCount_unfold_selectXkeysX_unfold() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.outE.weight.groupCount.unfold.select(keys).unfold")
        }

        @Override
        public Traversal<Vertex, Collection<Set<String>>> get_g_V_asXa_bX_out_asXcX_path_selectXkeysX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a','b').out.as('c').path.select(keys)")
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_outXknowsX_asXbX_localXselectXa_bX_byXnameXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').out('knows').as('b').local(select('a', 'b').by('name'))")
        }
    }
}
