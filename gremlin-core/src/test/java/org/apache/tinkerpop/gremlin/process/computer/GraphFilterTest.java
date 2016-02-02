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

package org.apache.tinkerpop.gremlin.process.computer;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphFilterTest {

    @Test
    public void shouldHandlePreFilterCorrectly() {
        GraphFilter graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>bothE().limit(0));
        assertTrue(graphFilter.allowedEdgeLabels.isEmpty());
        assertEquals(Direction.BOTH, graphFilter.allowedEdgeDirection);
        assertFalse(graphFilter.allowAllRemainingEdges);
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>bothE("knows").limit(0));
        assertEquals(1, graphFilter.allowedEdgeLabels.size());
        assertTrue(graphFilter.allowedEdgeLabels.contains("knows"));
        assertEquals(Direction.BOTH, graphFilter.allowedEdgeDirection);
        assertFalse(graphFilter.allowAllRemainingEdges);
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>outE("knows", "created"));
        assertEquals(2, graphFilter.allowedEdgeLabels.size());
        assertTrue(graphFilter.allowedEdgeLabels.contains("knows"));
        assertTrue(graphFilter.allowedEdgeLabels.contains("created"));
        assertEquals(Direction.OUT, graphFilter.allowedEdgeDirection);
        assertTrue(graphFilter.allowAllRemainingEdges);
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>inE("knows", "created", "likes"));
        assertEquals(3, graphFilter.allowedEdgeLabels.size());
        assertTrue(graphFilter.allowedEdgeLabels.contains("knows"));
        assertTrue(graphFilter.allowedEdgeLabels.contains("created"));
        assertTrue(graphFilter.allowedEdgeLabels.contains("likes"));
        assertEquals(Direction.IN, graphFilter.allowedEdgeDirection);
        assertTrue(graphFilter.allowAllRemainingEdges);
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>inE("knows", "created", "likes"));
        assertEquals(3, graphFilter.allowedEdgeLabels.size());
        assertTrue(graphFilter.allowedEdgeLabels.contains("knows"));
        assertTrue(graphFilter.allowedEdgeLabels.contains("created"));
        assertTrue(graphFilter.allowedEdgeLabels.contains("likes"));
        assertEquals(Direction.IN, graphFilter.allowedEdgeDirection);
        assertTrue(graphFilter.allowAllRemainingEdges);
        //
        graphFilter = new GraphFilter();
        try {
            graphFilter.setEdgeFilter(__.<Vertex>inE("likes").inV().outE().has("weight", 1));    // cannot leave local star graph
            fail();
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("local star graph"));
        }
        //
        graphFilter = new GraphFilter();
        try {
            graphFilter.setVertexFilter(__.out("likes"));    // cannot leave local vertex
            fail();
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("local vertex"));
        }
    }

    /*@Test
    public void shouldHandleStarGraph() {
        final StarGraph graph = StarGraph.open();
        final Vertex vertex = graph.addVertex("person");
        for (int i = 0; i < 10; i++) {
            vertex.addEdge("created", graph.addVertex(i < 5 ? "software" : "hardware"), "weight", 1);
        }
        final GraphFilter graphFilter = new GraphFilter();
    }*/
}
