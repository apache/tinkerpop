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

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PeerPressure;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class PeerPressureTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_peerPressure_hasXclusterX();

    public abstract Traversal<Vertex, Map<Object, Number>> get_g_V_peerPressure_withXpropertyName_clusterX_withXedges_outEXknowsXX_pageRankX1X_withXpropertyName_rankX_withXedges_outEXknowsXX_withXtimes_2X_group_byXclusterX_byXrank_sumX_limitX100X();

    public abstract Traversal<Vertex, Map<Object, List<Object>>> get_g_V_hasXname_rippleX_inXcreatedX_peerPressure_withXedges_outEX_withXpropertyName_clusterX_repeatXunionXidentity__bothX_timesX2X_dedup_valueMapXname_clusterX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_peerPressure_hasXclusterX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_peerPressure_hasXclusterX();
        printTraversalForm(traversal);
        assertEquals(6, IteratorUtils.count(traversal));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_peerPressure_withXpropertyName_clusterX_withXedges_outEXknowsXX_pageRankX1X_withXpropertyName_rankX_withXedges_outEXknowsXX_withXtimes_2X_group_byXclusterX_byXrank_sumX_limitX100X() {
        final Traversal<Vertex, Map<Object, Number>> traversal = get_g_V_peerPressure_withXpropertyName_clusterX_withXedges_outEXknowsXX_pageRankX1X_withXpropertyName_rankX_withXedges_outEXknowsXX_withXtimes_2X_group_byXclusterX_byXrank_sumX_limitX100X();
        printTraversalForm(traversal);
        final Map<Object, Number> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(4, map.size());
        assertEquals(0.583d, (double) map.get(convertToVertexId("marko")), 0.001d);
        assertEquals(0.138d, (double) map.get(convertToVertexId("lop")), 0.001d);
        assertEquals(0.138d, (double) map.get(convertToVertexId("ripple")), 0.001d);
        assertEquals(0.138d, (double) map.get(convertToVertexId("peter")), 0.001d);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_rippleX_inXcreatedX_peerPressure_withXedges_outEX_withXpropertyName_clusterX_repeatXunionXidentity__bothX_timesX2X_dedup_valueMapXname_clusterX() {
        TestHelper.assumeNonDeterministic();
        final Traversal<Vertex, Map<Object, List<Object>>> traversal = get_g_V_hasXname_rippleX_inXcreatedX_peerPressure_withXedges_outEX_withXpropertyName_clusterX_repeatXunionXidentity__bothX_timesX2X_dedup_valueMapXname_clusterX();
        printTraversalForm(traversal);
        final List<Map<Object, List<Object>>> results = traversal.toList();
        assertEquals(6, results.size());
        final Map<String, Object> clusters = new HashMap<>();
        results.forEach(m -> clusters.put((String) m.get("name").get(0), m.get("cluster").get(0)));
        assertEquals(2, results.get(0).size());
        assertEquals(6, clusters.size());
        assertEquals(clusters.get("josh"), clusters.get("ripple"));
        assertEquals(clusters.get("josh"), clusters.get("lop"));
        final Set<Object> ids = new HashSet<>(clusters.values());
        assertEquals(4, ids.size());
        assertTrue(ids.contains(convertToVertexId("marko")));
        assertTrue(ids.contains(convertToVertexId("vadas")));
        assertTrue(ids.contains(convertToVertexId("josh")));
        assertTrue(ids.contains(convertToVertexId("peter")));
    }

    public static class Traversals extends PeerPressureTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_peerPressure_hasXclusterX() {
            return g.V().peerPressure().has(PeerPressureVertexProgram.CLUSTER);
        }

        @Override
        public Traversal<Vertex, Map<Object, Number>> get_g_V_peerPressure_withXpropertyName_clusterX_withXedges_outEXknowsXX_pageRankX1X_withXpropertyName_rankX_withXedges_outEXknowsXX_withXtimes_2X_group_byXclusterX_byXrank_sumX_limitX100X() {
            return g.V().peerPressure().with(PeerPressure.propertyName, "cluster").with(PeerPressure.edges, __.outE("knows")).pageRank(1.0d).with(PageRank.propertyName, "rank").with(PageRank.edges, __.outE("knows")).with(PageRank.times, 1).<Object, Number>group().by("cluster").by(__.values("rank").sum()).limit(100);
        }

        @Override
        public Traversal<Vertex, Map<Object, List<Object>>> get_g_V_hasXname_rippleX_inXcreatedX_peerPressure_withXedges_outEX_withXpropertyName_clusterX_repeatXunionXidentity__bothX_timesX2X_dedup_valueMapXname_clusterX() {
            return g.V().has("name", "ripple").in("created").peerPressure().with(PeerPressure.edges,__.outE()).with(PeerPressure.propertyName, "cluster").repeat(__.union(__.identity(), __.both())).times(2).dedup().valueMap("name", "cluster");
        }
    }
}