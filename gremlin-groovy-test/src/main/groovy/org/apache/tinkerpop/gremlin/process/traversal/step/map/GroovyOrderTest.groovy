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
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyOrderTest {

    public static class Traversals extends OrderTest {

        @Override
        public Traversal<Vertex, String> get_g_V_name_order() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().name.order()")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_order_byXa1_b1X_byXb2_a2X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.name.order.by { a, b -> a[1] <=> b[1] }.by { a, b -> b[2] <=> a[2] }")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_order_byXname_incrX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.order.by('name', incr).name")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_order_byXnameX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.order.by('name').name")
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_outE_order_byXweight_decrX_weight() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.outE.order.by('weight', Order.decr).weight")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_order_byXname_a1_b1X_byXname_b2_a2X_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.order.by('name', { a, b -> a[1].compareTo(b[1]) }).by('name', { a, b -> b[2].compareTo(a[2]) }).name")
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_asXaX_outXcreatedX_asXbX_order_byXshuffleX_selectXa_bX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('a').out('created').as('b').order.by(shuffle).select('a','b')")
        }

        @Override
        public Traversal<Vertex, Map<Integer, Integer>> get_g_VX1X_hasXlabel_personX_mapXmapXint_ageXX_orderXlocalX_byXvalues_decrX_byXkeys_incrX(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", """g.V(v1Id).hasLabel("person").map {
                final Map map = [:];
                map[1] = it.age;
                map[2] = it.age * 2;
                map[3] = it.age * 3;
                map[4] = it.age;
                return map;
            }.order(local).by(values,decr).by(keys,incr)""", "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_order_byXoutE_count__decrX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.order.by(__.outE.count, decr)")
        }

        @Override
        public Traversal<Vertex, Map<String, List<Vertex>>> get_g_V_group_byXlabelX_byXname_order_byXdecrX_foldX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.group.by(label).by(values('name').order().by(decr).fold())")
        }

        @Override
        public Traversal<Vertex, List<Double>> get_g_V_localXbothE_weight_foldX_order_byXsumXlocalX_decrX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.local(__.bothE.weight.fold).order.by(sum(local), decr)")
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXvX_mapXbothE_weight_foldX_sumXlocalX_asXsX_selectXv_sX_order_byXselectXsX_decrX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.as('v').map(__.bothE.weight.fold).sum(local).as('s').select('v', 's').order.by(select('s'),decr)")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasLabelXpersonX_order_byXageX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('person').order.by('age')")
        }

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_V_hasLabelXpersonX_fold_orderXlocalX_byXageX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('person').fold.order(local).by('age')")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasLabelXpersonX_order_byXvalueXageX__decrX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('person').order.by({it.age},decr).name")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_properties_order_byXkey_decrX_key() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.properties().order.by(key, decr).key")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXsong_name_OHBOYX_outXfollowedByX_outXfollowedByX_order_byXperformancesX_byXsongType_incrX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('song', 'name', 'OH BOY').out('followedBy').out('followedBy').order.by('performances').by('songType',decr)")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_hasLabelXpersonX_order_byXage_decrX_limitX5X_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.both.hasLabel('person').order.by('age',decr).limit(5).name")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_hasLabelXpersonX_order_byXage_decrX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.both.hasLabel('person').order.by('age',decr).name")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasLabelXsongX_order_byXperfomances_decrX_byXnameX_rangeX110_120X_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('song').order.by('performances',decr).by('name').range(110, 120).name")
        }

        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXpersonX_group_byXnameX_byXoutE_weight_sumX_orderXlocalX_byXvaluesX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('person').group.by('name').by(__.outE.weight.sum).order(local).by(values)")
        }

        @Override
        public Traversal<Vertex, Map.Entry<String, Number>> get_g_V_hasLabelXpersonX_group_byXnameX_byXoutE_weight_sumX_unfold_order_byXvalues_decrX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('person').group.by('name').by(__.outE.weight.sum).unfold.order.by(values, decr)")
        }
    }
}
