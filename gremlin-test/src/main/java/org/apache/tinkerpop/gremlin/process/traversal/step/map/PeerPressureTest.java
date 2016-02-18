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
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class PeerPressureTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_peerPressure();

    public abstract Traversal<Vertex, Map<Object, Number>> get_g_V_peerPressure_byXclusterX_byXoutEXknowsXX_pageRankX1X_byXrankX_byXoutEXknowsXX_timesX2X_group_byXclusterX_byXrank_sumX_limitX100X();


    @Test
    @LoadGraphWith(MODERN)
    public void g_V_peerPressure() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_peerPressure();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Vertex vertex = traversal.next();
            counter++;
            assertTrue(vertex.property(PeerPressureVertexProgram.CLUSTER).isPresent());
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_peerPressure_byXclusterX_byXoutEXknowsXX_pageRankX1X_byXrankX_byXoutEXknowsXX_timesX2X_group_byXclusterX_byXrank_sumX() {
        final Traversal<Vertex, Map<Object, Number>> traversal = get_g_V_peerPressure_byXclusterX_byXoutEXknowsXX_pageRankX1X_byXrankX_byXoutEXknowsXX_timesX2X_group_byXclusterX_byXrank_sumX_limitX100X();
        printTraversalForm(traversal);
        final Map<Object, Number> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(4, map.size());
        assertEquals(1.0d, (double) map.get(convertToVertexId("marko")), 0.001d);
        assertEquals(0.0d, (double) map.get(convertToVertexId("lop")), 0.001d);
        assertEquals(0.0d, (double) map.get(convertToVertexId("ripple")), 0.001d);
        assertEquals(0.0d, (double) map.get(convertToVertexId("peter")), 0.001d);
    }


    public static class Traversals extends PeerPressureTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_peerPressure() {
            return g.V().peerPressure();
        }

        @Override
        public Traversal<Vertex, Map<Object, Number>> get_g_V_peerPressure_byXclusterX_byXoutEXknowsXX_pageRankX1X_byXrankX_byXoutEXknowsXX_timesX2X_group_byXclusterX_byXrank_sumX_limitX100X() {
            return g.V().peerPressure().by("cluster").by(__.outE("knows")).pageRank(1.0d).by("rank").by(__.outE("knows")).times(1).<Object, Number>group().by("cluster").by(__.values("rank").sum()).limit(100);
        }
    }
}