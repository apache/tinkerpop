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

package org.apache.tinkerpop.gremlin.neo4j.structure.simple;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.neo4j.process.traversal.strategy.optimization.Neo4jGraphStepStrategy;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jEdge;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jHelper;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.neo4j.tinkerpop.api.Neo4jGraphAPI;
import org.neo4j.tinkerpop.api.Neo4jNode;
import org.neo4j.tinkerpop.api.Neo4jRelationship;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SimpleNeo4jGraph extends Neo4jGraph {

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, SimpleNeo4jGraph.class.getName());
        this.setProperty(Neo4jGraph.CONFIG_META_PROPERTIES, false);
        this.setProperty(Neo4jGraph.CONFIG_MULTI_PROPERTIES, false);
    }};

    static {
        TraversalStrategies.GlobalCache.registerStrategies(SimpleNeo4jGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(Neo4jGraphStepStrategy.instance()));
    }

    public SimpleNeo4jGraph(final Configuration configuration) {
        super(configuration);
        this.features = new SimpleNeo4jGraphFeatures();
    }

    public SimpleNeo4jGraph(final Neo4jGraphAPI baseGraph) {
        super(baseGraph, EMPTY_CONFIGURATION);
        this.features = new SimpleNeo4jGraphFeatures();
    }

    @Override
    public Neo4jVertex createVertex(final Neo4jNode node) {
        return new SimpleNeo4jVertex(node, this);
    }

    @Override
    public Neo4jEdge createEdge(final Neo4jRelationship relationship) {
        return new Neo4jEdge(relationship, this);
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        this.tx().readWrite();
        if (0 == vertexIds.length) {
            return IteratorUtils.stream(this.getBaseGraph().allNodes())
                    .filter(node -> !this.checkElementsInTransaction || !Neo4jHelper.isDeleted(node))
                    .map(node -> (Vertex) new SimpleNeo4jVertex(node, this)).iterator();
        } else {
            return Stream.of(vertexIds)
                    .filter(id -> id instanceof Number)
                    .flatMap(id -> {
                        try {
                            return Stream.of((Vertex) new SimpleNeo4jVertex(this.getBaseGraph().getNodeById(((Number) id).longValue()), this));
                        } catch (final RuntimeException e) {
                            if (Neo4jHelper.isNotFound(e)) return Stream.empty();
                            throw e;
                        }
                    }).iterator();
        }
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        this.tx().readWrite();
        if (0 == edgeIds.length) {
            return IteratorUtils.stream(this.getBaseGraph().allRelationships())
                    .filter(relationship -> !this.checkElementsInTransaction || !Neo4jHelper.isDeleted(relationship))
                    .map(relationship -> (Edge) this.createEdge(relationship)).iterator();
        } else {
            return Stream.of(edgeIds)
                    .filter(id -> id instanceof Number)
                    .flatMap(id -> {
                        try {
                            return Stream.of((Edge) this.createEdge(this.getBaseGraph().getRelationshipById(((Number) id).longValue())));
                        } catch (final RuntimeException e) {
                            if (Neo4jHelper.isNotFound(e)) return Stream.empty();
                            throw e;
                        }
                    }).iterator();
        }
    }

    ////////////////////////

    public class SimpleNeo4jGraphFeatures extends Neo4jGraphFeatures {

        public SimpleNeo4jGraphFeatures() {
            this.vertexFeatures = new SimpleNeo4jVertexFeatures();
        }

        public class SimpleNeo4jVertexFeatures extends Neo4jVertexFeatures {

            SimpleNeo4jVertexFeatures() {
                super();
            }

            public VertexProperty.Cardinality getCardinality(final String key) {
                return VertexProperty.Cardinality.single;
            }
        }
    }
}