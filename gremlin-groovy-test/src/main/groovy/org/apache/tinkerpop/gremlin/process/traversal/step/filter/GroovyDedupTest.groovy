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

import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalScriptHelper
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class GroovyDedupTest {

    public static class Traversals extends DedupTest {

        @Override
        public Traversal<Vertex, String> get_g_V_both_dedup_name() {
            TraversalScriptHelper.compute("g.V.both.dedup.name", g);
        }

        @Override
        public Traversal<Vertex, Map<String, List<Double>>> get_g_V_group_byXlabelX_byXbothE_weight_dedup_foldX() {
            TraversalScriptHelper.compute("g.V.group.by(label).by(__.bothE.weight.dedup.fold)", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_hasXlabel_softwareX_dedup_byXlangX_name() {
            TraversalScriptHelper.compute("g.V.both.has(label, 'software').dedup.by('lang').name", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_name_order_byXa_bX_dedup_value() {
            TraversalScriptHelper.compute("g.V().both().properties('name').order.by { a, b -> a.value() <=> b.value() }.dedup.value", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_both_both_dedup() {
            TraversalScriptHelper.compute("g.V.both.both.dedup", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_both_both_dedup_byXlabelX() {
            TraversalScriptHelper.compute("g.V.both.both.dedup.by(label)", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_both_name_dedup() {
            TraversalScriptHelper.compute("g.V.both.both.name.dedup", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_asXaX_both_asXbX_dedupXa_bX_byXlabelX_selectXa_bX() {
            TraversalScriptHelper.compute("g.V.as('a').both.as('b').dedup('a', 'b').by(label).select('a','b')", g)
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_dedupXa_bX_path() {
            TraversalScriptHelper.compute("g.V.as('a').out('created').as('b').in('created').as('c').dedup('a','b').path", g)
        }

        @Override
        Traversal<Vertex, String> get_g_V_outE_asXeX_inV_asXvX_selectXeX_order_byXweight_incrX_selectXvX_valuesXnameX_dedup() {
            TraversalScriptHelper.compute("g.V.outE.as('e').inV.as('v').select('e').order.by('weight', incr).select('v').values('name').dedup", g)
        }
    }
}
