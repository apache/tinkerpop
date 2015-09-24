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
package org.apache.tinkerpop.gremlin.neo4j;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.neo4j.tinkerpop.api.Neo4jGraphAPI;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AbstractNeo4jGremlinTest extends AbstractGremlinTest {

    protected Neo4jGraph getGraph() {
        return (Neo4jGraph) this.graph;
    }

    protected Neo4jGraphAPI getBaseGraph() {
        return ((Neo4jGraph) this.graph).getBaseGraph();
    }

    protected void validateCounts(int gV, int gE, int gN, int gR) {
        assertEquals(gV, IteratorUtils.count(graph.vertices()));
        assertEquals(gE, IteratorUtils.count(graph.edges()));
        assertEquals(gN, IteratorUtils.count(this.getBaseGraph().allNodes()));
        assertEquals(gR, IteratorUtils.count(this.getBaseGraph().allRelationships()));
    }
}
