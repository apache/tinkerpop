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

package org.apache.tinkerpop.gremlin.process.computer

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.structure.Direction
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.junit.Test

import static org.junit.Assert.*

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroovyGraphFilterTest {

    @Test
    public void shouldHandlePreFilterCorrectly() {
        GraphFilter graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex> bothE().limit(0));
        assertTrue(graphFilter.allowNoEdges);
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex> bothE("knows").limit(0));
        assertTrue(graphFilter.allowNoEdges);
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex> bothE("knows").select("a"));
        assertEquals(1, graphFilter.allowedEdgeLabels.size());
        assertTrue(graphFilter.allowedEdgeLabels.contains("knows"));
        assertEquals(Direction.BOTH, graphFilter.allowedEdgeDirection);
        //assertFalse(graphFilter.allowAllRemainingEdges);
        assertFalse(graphFilter.allowNoEdges);
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex> outE("knows", "created"));
        assertEquals(2, graphFilter.allowedEdgeLabels.size());
        assertTrue(graphFilter.allowedEdgeLabels.contains("knows"));
        assertTrue(graphFilter.allowedEdgeLabels.contains("created"));
        assertEquals(Direction.OUT, graphFilter.allowedEdgeDirection);
        //assertTrue(graphFilter.allowAllRemainingEdges);
        assertFalse(graphFilter.allowNoEdges);
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex> inE("knows", "created", "likes"));
        assertEquals(3, graphFilter.allowedEdgeLabels.size());
        assertTrue(graphFilter.allowedEdgeLabels.contains("knows"));
        assertTrue(graphFilter.allowedEdgeLabels.contains("created"));
        assertTrue(graphFilter.allowedEdgeLabels.contains("likes"));
        assertEquals(Direction.IN, graphFilter.allowedEdgeDirection);
        //assertTrue(graphFilter.allowAllRemainingEdges);
        assertFalse(graphFilter.allowNoEdges);
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex> inE("knows", "created", "likes"));
        assertEquals(3, graphFilter.allowedEdgeLabels.size());
        assertTrue(graphFilter.allowedEdgeLabels.contains("knows"));
        assertTrue(graphFilter.allowedEdgeLabels.contains("created"));
        assertTrue(graphFilter.allowedEdgeLabels.contains("likes"));
        assertEquals(Direction.IN, graphFilter.allowedEdgeDirection);
        //assertTrue(graphFilter.allowAllRemainingEdges);
        assertFalse(graphFilter.allowNoEdges);
        //
        graphFilter = new GraphFilter();
        try {
            graphFilter.setVertexFilter(__.out("likes"));    // cannot leave local vertex
            fail();
        } catch (final IllegalArgumentException e) {
            assertEquals(e.getMessage(), GraphComputer.Exceptions.vertexFilterAccessesIncidentEdges(__.out("likes")).getMessage());
        }
        //
        graphFilter = new GraphFilter();
        try {
            graphFilter.setEdgeFilter(__.<Vertex> inE("likes").inV().outE().has("weight", 1));
            // cannot leave local star graph
            fail();
        } catch (final IllegalArgumentException e) {
            assertEquals(e.getMessage(), GraphComputer.Exceptions.edgeFilterAccessesAdjacentVertices(__.<Vertex> inE("likes").inV().outE().has("weight", 1)).getMessage());
        }
    }
}
