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
public abstract class GroovyPageRankTest {

    public static class Traversals extends PageRankTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_pageRank_hasXpageRankX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.pageRank.has(PageRankVertexProgram.PAGE_RANK)")
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_pageRank_byXoutEXknowsXX_byXfriendRankX_valueMapXname_friendRankX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.pageRank.by(outE('knows')).by('friendRank').valueMap('name','friendRank')")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_pageRank_order_byXpageRank_decrX_byXnameX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.pageRank.order.by(PageRankVertexProgram.PAGE_RANK, decr).by('name').name")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_pageRank_order_byXpageRank_decrX_name_limitX2X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.pageRank.order.by(PageRankVertexProgram.PAGE_RANK, decr).name.limit(2)")
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasLabelXpersonX_pageRank_byXpageRankX_order_byXpageRankX_valueMapXname_pageRankX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('person').pageRank.by('pageRank').order.by('pageRank').valueMap('name', 'pageRank')")
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_pageRank_byXpageRankX_asXaX_outXknowsX_pageRank_asXbX_selectXa_bX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.pageRank.by('pageRank').as('a').out('knows').values('pageRank').as('b').select('a', 'b')")
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasLabelXsoftwareX_hasXname_rippleX_pageRankX1X_byXinEXcreatedXX_timesX1X_byXpriorsX_inXcreatedX_unionXboth__identityX_valueMapXname_priorsX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().hasLabel('software').has('name', 'ripple').pageRank(1.0).by(inE('created')).times(1).by('priors').in('created').union(identity(),both()).valueMap('name', 'priors')")
        }

        @Override
        public Traversal<Vertex, Map<Object, List<Vertex>>> get_g_V_outXcreatedX_groupXmX_byXlabelX_pageRankX1X_byXpageRankX_byXinEX_timesX1X_inXcreatedX_groupXmX_byXpageRankX_capXmX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out('created').group('m').by(label).pageRank(1.0).by('pageRank').by(inE()).times(1).in('created').group('m').by('pageRank').cap('m')")
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_outXcreatedX_pageRank_byXbothEX_byXprojectRankX_timesX0X_valueMapXname_projectRankX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out('created').pageRank().by(bothE()).by('projectRank').times(0).valueMap('name','projectRank')")
        }
    }
}
