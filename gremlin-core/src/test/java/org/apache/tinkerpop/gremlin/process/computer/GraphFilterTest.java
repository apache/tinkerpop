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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphFilterTest {

    @Test
    public void shouldHaveValidLegalEnumOrdering() {
        assertTrue(GraphFilter.Legal.YES.compareTo(GraphFilter.Legal.YES) == 0);
        assertTrue(GraphFilter.Legal.YES.compareTo(GraphFilter.Legal.NO) < 0);
        assertTrue(GraphFilter.Legal.YES.compareTo(GraphFilter.Legal.MAYBE) < 0);
        assertTrue(GraphFilter.Legal.MAYBE.compareTo(GraphFilter.Legal.NO) < 0);
    }

    @Test
    public void shouldOnlyAllowEdgeFilterToTraverseLocalStarGraph() {
        GraphFilter graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.outE());
        try {
            graphFilter.setEdgeFilter(__.<Vertex>outE().inV().outE());
            fail("Should not allow traversals past the star graph");
        } catch (final IllegalArgumentException e) {
            assertEquals(e.getMessage(), GraphComputer.Exceptions.edgeFilterAccessesAdjacentVertices(__.<Vertex>outE().inV().outE()).getMessage());
        }
    }

    @Test
    public void shouldOnlyAllowVertexFilterToTraverseVertex() {
        GraphFilter graphFilter = new GraphFilter();
        graphFilter.setVertexFilter(__.hasLabel("person"));
        try {
            graphFilter.setVertexFilter(__.<Vertex>as("a").outE().<Vertex>select("a"));
            fail("Should not allow traversals to the incident edges");
        } catch (final IllegalArgumentException e) {
            assertEquals(e.getMessage(), GraphComputer.Exceptions.vertexFilterAccessesIncidentEdges(__.<Vertex>as("a").outE().<Vertex>select("a")).getMessage());
        }
    }

    @Test
    public void shouldGetLegallyPositiveEdgeLabels() {
        GraphFilter graphFilter = new GraphFilter();
        assertFalse(graphFilter.hasEdgeFilter());
        assertEquals(Collections.singleton(null), graphFilter.getLegallyPositiveEdgeLabels(Direction.OUT));
        assertEquals(Collections.singleton(null), graphFilter.getLegallyPositiveEdgeLabels(Direction.IN));
        assertEquals(Collections.singleton(null), graphFilter.getLegallyPositiveEdgeLabels(Direction.BOTH));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.outE("created"));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(Collections.singleton("created"), graphFilter.getLegallyPositiveEdgeLabels(Direction.OUT));
        assertEquals(Collections.emptySet(), graphFilter.getLegallyPositiveEdgeLabels(Direction.IN));
        assertEquals(Collections.emptySet(), graphFilter.getLegallyPositiveEdgeLabels(Direction.BOTH));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.outE());
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(Collections.singleton(null), graphFilter.getLegallyPositiveEdgeLabels(Direction.OUT));
        assertEquals(Collections.emptySet(), graphFilter.getLegallyPositiveEdgeLabels(Direction.IN));
        assertEquals(Collections.emptySet(), graphFilter.getLegallyPositiveEdgeLabels(Direction.BOTH));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>outE("created").has("weight", 32));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(Collections.singleton("created"), graphFilter.getLegallyPositiveEdgeLabels(Direction.OUT));
        assertEquals(Collections.emptySet(), graphFilter.getLegallyPositiveEdgeLabels(Direction.IN));
        assertEquals(Collections.emptySet(), graphFilter.getLegallyPositiveEdgeLabels(Direction.BOTH));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>identity().outE("created"));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(Collections.singleton(null), graphFilter.getLegallyPositiveEdgeLabels(Direction.OUT));
        assertEquals(Collections.singleton(null), graphFilter.getLegallyPositiveEdgeLabels(Direction.IN));
        assertEquals(Collections.singleton(null), graphFilter.getLegallyPositiveEdgeLabels(Direction.BOTH));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.bothE());
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(Collections.singleton(null), graphFilter.getLegallyPositiveEdgeLabels(Direction.OUT));
        assertEquals(Collections.singleton(null), graphFilter.getLegallyPositiveEdgeLabels(Direction.IN));
        assertEquals(Collections.singleton(null), graphFilter.getLegallyPositiveEdgeLabels(Direction.BOTH));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>bothE().has("weight", 32));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(Collections.singleton(null), graphFilter.getLegallyPositiveEdgeLabels(Direction.OUT));
        assertEquals(Collections.singleton(null), graphFilter.getLegallyPositiveEdgeLabels(Direction.IN));
        assertEquals(Collections.singleton(null), graphFilter.getLegallyPositiveEdgeLabels(Direction.BOTH));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>bothE().limit(0));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(Collections.emptySet(), graphFilter.getLegallyPositiveEdgeLabels(Direction.OUT));
        assertEquals(Collections.emptySet(), graphFilter.getLegallyPositiveEdgeLabels(Direction.IN));
        assertEquals(Collections.emptySet(), graphFilter.getLegallyPositiveEdgeLabels(Direction.BOTH));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>bothE("created").has("weight", 32));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(Collections.singleton("created"), graphFilter.getLegallyPositiveEdgeLabels(Direction.OUT));
        assertEquals(Collections.singleton("created"), graphFilter.getLegallyPositiveEdgeLabels(Direction.IN));
        assertEquals(Collections.singleton("created"), graphFilter.getLegallyPositiveEdgeLabels(Direction.BOTH));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.union(__.outE("created"), __.inE("likes")));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(Collections.singleton("created"), graphFilter.getLegallyPositiveEdgeLabels(Direction.OUT));
        assertEquals(Collections.singleton("likes"), graphFilter.getLegallyPositiveEdgeLabels(Direction.IN));
        assertEquals(Collections.emptySet(), graphFilter.getLegallyPositiveEdgeLabels(Direction.BOTH));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.union(__.outE("created"), __.inE("likes", "created")));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(Collections.singleton("created"), graphFilter.getLegallyPositiveEdgeLabels(Direction.OUT));
        assertEquals(new HashSet<>(Arrays.asList("likes", "created")), graphFilter.getLegallyPositiveEdgeLabels(Direction.IN));
        assertEquals(Collections.singleton("created"), graphFilter.getLegallyPositiveEdgeLabels(Direction.BOTH));
    }

    @Test
    public void shouldHaveProperEdgeLegality() {
        GraphFilter graphFilter = new GraphFilter();
        assertFalse(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN, "created"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.BOTH, "likes"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT, "knows"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.flatMap(v -> v.get().edges(Direction.OUT, "created")));  // lambdas can not be introspected
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.IN, "created"));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.BOTH, "likes"));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT, "knows"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>identity().bothE());  // there are no strategies for __.
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.IN, "created"));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.BOTH, "likes"));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT, "knows"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>bothE().has("weight", 32));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.IN, "created"));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.BOTH, "likes"));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT, "knows"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>inE().has("weight", 32));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.IN, "created"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.BOTH, "likes"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.OUT, "knows"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>bothE().limit(0));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.OUT));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.outE());
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT, "knows"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>outE().limit(10));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT, "knows"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.<Vertex>outE("knows").limit(10));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT, "knows"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.OUT, "created"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.bothE("created"));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.BOTH, "created"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN, "created"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.BOTH, "knows"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.OUT, "knows"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.outE("knows", "likes"));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.BOTH, "likes"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.BOTH, "created"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT, "knows"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT, "likes"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.OUT, "created"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.union(__.inE("bought"), __.outE("created"), __.bothE("knows", "likes")));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT, "created"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT, "likes"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.IN, "created"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.OUT, "worksFor"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.union(__.outE("created"), __.bothE("knows", "likes")));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT, "created"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT, "likes"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.IN, "created"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN, "likes"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.OUT, "worksFor"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.union(__.inE("bought").has("weight", 32), __.outE("created"), __.bothE("knows", "likes")));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT, "created"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT, "likes"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.IN, "created"));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.IN, "bought"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.OUT, "worksFor"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN, "likes"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN, "knows"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.union(__.inE("bought").has("weight", 32), __.outE("created"), __.bothE("knows", "likes", "bought")));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT, "created"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT, "likes"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.OUT, "blah"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.IN, "created"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN, "bought"));
        assertEquals(GraphFilter.Legal.NO, graphFilter.checkEdgeLegality(Direction.OUT, "worksFor"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN, "likes"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN, "knows"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.union(__.outE("created").limit(0), __.inE("created")));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT, "created"));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN, "created"));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.BOTH, "created"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.union(__.outE(), __.inE().limit(0)));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT, "created"));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.IN, "created"));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.BOTH, "created"));
    }
}