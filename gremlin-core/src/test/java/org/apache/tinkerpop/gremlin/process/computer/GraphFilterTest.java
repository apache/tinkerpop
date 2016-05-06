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

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphFilterTest {

    @Test
    public void shouldHaveProperEdgeLegality() {
        GraphFilter graphFilter = new GraphFilter();
        assertFalse(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.YES, graphFilter.checkEdgeLegality(Direction.OUT));
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
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT, "knows"));
        //
        graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.union(__.inE("bought"), __.outE("created"), __.bothE("knows", "likes")));
        assertTrue(graphFilter.hasEdgeFilter());
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.IN));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.BOTH));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT, "created"));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT, "likes"));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.IN, "created"));
        assertEquals(GraphFilter.Legal.MAYBE, graphFilter.checkEdgeLegality(Direction.OUT, "worksFor"));
    }
}