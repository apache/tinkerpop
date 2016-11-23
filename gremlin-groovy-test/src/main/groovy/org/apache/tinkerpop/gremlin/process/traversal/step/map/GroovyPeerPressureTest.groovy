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
public abstract class GroovyPeerPressureTest {

    public static class Traversals extends PeerPressureTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_peerPressure() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.peerPressure")
        }

        @Override
        public Traversal<Vertex, Map<Object, Number>> get_g_V_peerPressure_byXclusterX_byXoutEXknowsXX_pageRankX1X_byXrankX_byXoutEXknowsXX_timesX2X_group_byXclusterX_byXrank_sumX_limitX100X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.peerPressure.by('cluster').by(outE('knows')).pageRank(1.0).by('rank').by(outE('knows')).times(1).group.by('cluster').by(values('rank').sum).limit(100)")
        }

        @Override
        public Traversal<Vertex, Map<Object, List<Object>>> get_g_V_hasXname_rippleX_inXcreatedX_peerPressure_byXoutEX_byXclusterX_repeatXunionXidentity__bothX_timesX2X_dedup_valueMapXname_clusterX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('name', 'ripple').in('created').peerPressure.by(outE()).by('cluster').repeat(union(identity(), both())).times(2).dedup.valueMap('name', 'cluster')")
        }
    }
}
