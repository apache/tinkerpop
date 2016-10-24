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

package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.decoration;

import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferencePath;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HaltedTraverserStrategyTest {

    @Before
    public void setup() {
        // necessary as ComputerResult step for testing purposes attaches Attachables
        System.setProperty("is.testing", "false");
    }

    @After
    public void shutdown() {
        System.setProperty("is.testing", "true");
    }

    @Test
    public void shouldReturnDetachedElements() {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal().withComputer().withStrategies(HaltedTraverserStrategy.create(new MapConfiguration(new HashMap<String, Object>() {{
            put(HaltedTraverserStrategy.HALTED_TRAVERSER_FACTORY, DetachedFactory.class.getCanonicalName());
        }})));
        g.V().out().forEachRemaining(vertex -> assertEquals(DetachedVertex.class, vertex.getClass()));
        g.V().out().properties("name").forEachRemaining(vertexProperty -> assertEquals(DetachedVertexProperty.class, vertexProperty.getClass()));
        g.V().out().values("name").forEachRemaining(value -> assertEquals(String.class, value.getClass()));
        g.V().out().outE().forEachRemaining(edge -> assertEquals(DetachedEdge.class, edge.getClass()));
        g.V().out().outE().properties("weight").forEachRemaining(property -> assertEquals(DetachedProperty.class, property.getClass()));
        g.V().out().outE().values("weight").forEachRemaining(value -> assertEquals(Double.class, value.getClass()));
        g.V().out().out().forEachRemaining(vertex -> assertEquals(DetachedVertex.class, vertex.getClass()));
        g.V().out().out().path().forEachRemaining(path -> assertEquals(DetachedPath.class, path.getClass()));
        g.V().out().pageRank().forEachRemaining(vertex -> assertEquals(DetachedVertex.class, vertex.getClass()));
        g.V().out().pageRank().out().forEachRemaining(vertex -> assertEquals(DetachedVertex.class, vertex.getClass()));
    }

    @Test
    public void shouldReturnReferenceElements() {
        final Graph graph = TinkerFactory.createModern();
        GraphTraversalSource g = graph.traversal().withComputer().withStrategies(HaltedTraverserStrategy.reference());
        g.V().out().forEachRemaining(vertex -> assertEquals(ReferenceVertex.class, vertex.getClass()));
        g.V().out().properties("name").forEachRemaining(vertexProperty -> assertEquals(ReferenceVertexProperty.class, vertexProperty.getClass()));
        g.V().out().values("name").forEachRemaining(value -> assertEquals(String.class, value.getClass()));
        g.V().out().outE().forEachRemaining(edge -> assertEquals(ReferenceEdge.class, edge.getClass()));
        g.V().out().outE().properties("weight").forEachRemaining(property -> assertEquals(ReferenceProperty.class, property.getClass()));
        g.V().out().outE().values("weight").forEachRemaining(value -> assertEquals(Double.class, value.getClass()));
        g.V().out().out().forEachRemaining(vertex -> assertEquals(ReferenceVertex.class, vertex.getClass()));
        g.V().out().out().path().forEachRemaining(path -> assertEquals(ReferencePath.class, path.getClass()));
        g.V().out().pageRank().forEachRemaining(vertex -> assertEquals(ReferenceVertex.class, vertex.getClass()));
        g.V().out().pageRank().out().forEachRemaining(vertex -> assertEquals(ReferenceVertex.class, vertex.getClass()));
        // the default should be reference elements
        g = graph.traversal().withComputer();
        g.V().out().forEachRemaining(vertex -> assertEquals(ReferenceVertex.class, vertex.getClass()));
        g.V().out().properties("name").forEachRemaining(vertexProperty -> assertEquals(ReferenceVertexProperty.class, vertexProperty.getClass()));
        g.V().out().values("name").forEachRemaining(value -> assertEquals(String.class, value.getClass()));
        g.V().out().outE().forEachRemaining(edge -> assertEquals(ReferenceEdge.class, edge.getClass()));
        g.V().out().outE().properties("weight").forEachRemaining(property -> assertEquals(ReferenceProperty.class, property.getClass()));
        g.V().out().outE().values("weight").forEachRemaining(value -> assertEquals(Double.class, value.getClass()));
        g.V().out().out().forEachRemaining(vertex -> assertEquals(ReferenceVertex.class, vertex.getClass()));
        g.V().out().out().path().forEachRemaining(path -> assertEquals(ReferencePath.class, path.getClass()));
        g.V().out().pageRank().forEachRemaining(vertex -> assertEquals(ReferenceVertex.class, vertex.getClass()));
        g.V().out().pageRank().out().forEachRemaining(vertex -> assertEquals(ReferenceVertex.class, vertex.getClass()));
    }

}
