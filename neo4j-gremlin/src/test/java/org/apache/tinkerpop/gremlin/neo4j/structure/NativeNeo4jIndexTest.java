/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.neo4j.structure;

import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.neo4j.AbstractNeo4jGremlinTest;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class NativeNeo4jIndexTest extends AbstractNeo4jGremlinTest {

    @Test
    public void shouldHaveFasterRuntimeWithLabelKeyValueIndex() throws Exception {
        final Neo4jGraph neo4j = (Neo4jGraph) this.graph;
        int maxVertices = 10000;
        for (int i = 0; i < maxVertices; i++) {
            if (i % 2 == 0)
                this.graph.addVertex(T.label, "something", "myId", i);
            else
                this.graph.addVertex(T.label, "nothing", "myId", i);
        }
        this.graph.tx().commit();

        // traversal
        final Runnable traversal = () -> {
            final Traversal<Vertex, Vertex> t = g.V().hasLabel("something").has("myId", 2000);
            final Vertex vertex = t.tryNext().get();
            assertFalse(t.hasNext());
            assertEquals(1, IteratorUtils.count(vertex.properties("myId")));
            assertEquals("something", vertex.label());
        };

        // no index
        this.graph.tx().readWrite();
        assertFalse(this.getBaseGraph().hasSchemaIndex("something", "myId"));
        TimeUtil.clock(20, traversal);
        final double noIndexTime = TimeUtil.clock(20, traversal);
        // index time
        neo4j.cypher("CREATE INDEX ON :something(myId)").iterate();
        this.graph.tx().commit();
        this.graph.tx().readWrite();
        ((Neo4jGraph) this.graph).baseGraph.awaitIndexesOnline(1, TimeUnit.MINUTES);
        assertTrue(this.getBaseGraph().hasSchemaIndex("something", "myId"));
        TimeUtil.clock(20, traversal);
        final double indexTime = TimeUtil.clock(20, traversal);
        System.out.println("Query time (no-index vs. index): " + noIndexTime + " vs. " + indexTime);
        assertTrue((noIndexTime / 10) > indexTime); // should be at least 10x faster
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldHaveFasterRuntimeWithLabelKeyValueIndexOnMultiProperties() throws Exception {
        final Neo4jGraph neo4j = (Neo4jGraph) this.graph;
        int maxVertices = 10000;
        for (int i = 0; i < maxVertices; i++) {
            if (i % 2 == 0)
                this.graph.addVertex(T.label, "something", "myId", i, "myId", i + maxVertices + 1);
            else
                this.graph.addVertex(T.label, "nothing", "myId", i, "myId", i + maxVertices + 1);
        }
        this.graph.tx().commit();

        // traversal
        final Runnable traversal = () -> {
            final Traversal<Vertex, Vertex> t = g.V().hasLabel("something").has("myId", 2000 + maxVertices + 1);
            final Vertex vertex = t.tryNext().get();
            assertFalse(t.hasNext());
            assertEquals(2, IteratorUtils.count(vertex.properties("myId")));
            assertEquals("something", vertex.label());
        };

        // no index
        this.graph.tx().readWrite();
        assertFalse(this.getBaseGraph().hasSchemaIndex("something", "myId"));
        TimeUtil.clock(20, traversal);
        final double noIndexTime = TimeUtil.clock(20, traversal);
        // index time
        neo4j.cypher("CREATE INDEX ON :something(myId)").iterate();
        neo4j.cypher("CREATE INDEX ON :vertexProperty(myId)").iterate();
        this.graph.tx().commit();
        this.graph.tx().readWrite();
        ((Neo4jGraph) this.graph).baseGraph.awaitIndexesOnline(1, TimeUnit.MINUTES);
        assertTrue(this.getBaseGraph().hasSchemaIndex("something", "myId"));
        TimeUtil.clock(20, traversal);
        final double indexTime = TimeUtil.clock(20, traversal);
        System.out.println("Query time (no-index vs. index): " + noIndexTime + " vs. " + indexTime);
        assertTrue((noIndexTime / 10) > indexTime); // should be at least 10x faster
    }
}
