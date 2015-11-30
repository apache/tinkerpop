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
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyOrderTest {

    public static class Traversals extends OrderTest {

        @Override
        public Traversal<Vertex, String> get_g_V_name_order() {
            TraversalScriptHelper.compute("g.V().name.order()", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_order_byXa1_b1X_byXb2_a2X() {
            TraversalScriptHelper.compute("g.V.name.order.by { a, b -> a[1] <=> b[1] }.by { a, b -> b[2] <=> a[2] }", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_order_byXname_incrX_name() {
            TraversalScriptHelper.compute("g.V.order.by('name', incr).name", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_order_byXnameX_name() {
            TraversalScriptHelper.compute("g.V.order.by('name', incr).name", g)
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_outE_order_byXweight_decrX_weight() {
            TraversalScriptHelper.compute("g.V.outE.order.by('weight', Order.decr).weight", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_order_byXname_a1_b1X_byXname_b2_a2X_name() {
            TraversalScriptHelper.compute("g.V.order.by('name', { a, b -> a[1].compareTo(b[1]) }).by('name', { a, b -> b[2].compareTo(a[2]) }).name", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_asXaX_outXcreatedX_asXbX_order_byXshuffleX_selectXa_bX() {
            TraversalScriptHelper.compute("g.V.as('a').out('created').as('b').order.by(shuffle).select('a','b')", g)
        }

        @Override
        public Traversal<Vertex, Map<Integer, Integer>> get_g_VX1X_hasXlabel_personX_mapXmapXint_ageXX_orderXlocalX_byXvalues_decrX_byXkeys_incrX(
                final Object v1Id) {
            TraversalScriptHelper.compute("""g.V(v1Id).map {
                final Map map = [:];
                map[1] = it.age;
                map[2] = it.age * 2;
                map[3] = it.age * 3;
                map[4] = it.age;
                return map;
            }.order(local).by(values,decr).by(keys,incr)""", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_order_byXoutE_count__decrX() {
            TraversalScriptHelper.compute("g.V.order.by(__.outE.count, decr)", g)
        }

        @Override
        public Traversal<Vertex, Map<String, List<Vertex>>> get_g_V_group_byXlabelX_byXnameX_byXorderXlocalX_byXdecrXX() {
            TraversalScriptHelper.compute("g.V.group.by(label).by(values('name').order().by(decr).fold())", g)
        }

        @Override
        public Traversal<Vertex, List<Double>> get_g_V_localXbothE_weight_foldX_order_byXsumXlocalX_decrX() {
            TraversalScriptHelper.compute("g.V.local(__.bothE.weight.fold).order.by(sum(local), decr)", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXvX_mapXbothE_weight_foldX_sumXlocalX_asXsX_selectXv_sX_order_byXselectXsX_decrX() {
            TraversalScriptHelper.compute("g.V.as('v').map(__.bothE.weight.fold).sum(local).as('s').select('v', 's').order.by(select('s'),decr)", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasLabelXpersonX_order_byXageX() {
            TraversalScriptHelper.compute("g.V.hasLabel('person').order.by('age')", g);
        }

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_V_hasLabelXpersonX_fold_orderXlocalX_byXageX() {
            TraversalScriptHelper.compute("g.V.hasLabel('person').fold.order(local).by('age')", g)
        }
    }
}
