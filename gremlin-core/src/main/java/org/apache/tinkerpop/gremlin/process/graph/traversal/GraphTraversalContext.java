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
package org.apache.tinkerpop.gremlin.process.graph.traversal;

import org.apache.tinkerpop.gremlin.process.TraversalContext;
import org.apache.tinkerpop.gremlin.process.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphTraversalContext implements TraversalContext {

    private final Graph graph;
    private final TraversalEngine engine;
    private final TraversalStrategies strategies;

    public GraphTraversalContext(final Graph graph, final TraversalEngine engine, final TraversalStrategy... strategies) {
        this.graph = graph;
        this.engine = engine;
        this.strategies = TraversalStrategies.GlobalCache.getStrategies(this.graph.getClass()).addStrategies(strategies);
    }

    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(this);
        traversal.setEngine(this.engine);
        traversal.setStrategies(this.strategies);
        return traversal.addStep(new GraphStep<>(traversal, this.graph, Vertex.class, vertexIds));
    }

    public Transaction tx() {
        return this.graph.tx();
    }

    public static Builder of() {
        return new Builder();
    }

    //////

    public static class Builder implements TraversalContext.Builder<GraphTraversalContext> {

        private TraversalEngine engine = StandardTraversalEngine.standard;
        private List<TraversalStrategy> strategies = new ArrayList<>();

        public Builder engine(final TraversalEngine engine) {
            this.engine = engine;
            return this;
        }

        public Builder strategy(final TraversalStrategy strategy) {
            this.strategies.add(strategy);
            return this;
        }

        public GraphTraversalContext create(final Graph graph) {
            return new GraphTraversalContext(graph, this.engine, this.strategies.toArray(new TraversalStrategy[this.strategies.size()]));
        }


    }
}
