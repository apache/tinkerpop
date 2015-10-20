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
package org.apache.tinkerpop.gremlin.tinkergraph.process;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.engine.ComputerTraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConnectiveStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.EngineDependentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphNoStrategyComputerProvider extends TinkerGraphComputerProvider {

    private static final HashSet<Class<? extends TraversalStrategy>> REQUIRED_STRATEGIES = new HashSet<>(Arrays.asList(
            ComputerVerificationStrategy.class,
            EngineDependentStrategy.class,
            ProfileStrategy.class,
            ConnectiveStrategy.class,
            ComputerTraversalEngine.ComputerResultStrategy.class));

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        final GraphTraversalSource.Builder builder = createBuilder(graph);
        return builder.create(graph);
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph, final TraversalStrategy... strategies) {
        final GraphTraversalSource.Builder builder = createBuilder(graph);
        Arrays.asList(strategies).forEach(builder::with);
        return builder.create(graph);
    }

    private GraphTraversalSource.Builder createBuilder(Graph graph) {
        final GraphTraversalSource g = super.traversal(graph);
        final GraphTraversalSource.Builder builder = g.asBuilder();
        g.getStrategies().stream().map(strategy -> strategy.getClass()).filter(clazz -> !REQUIRED_STRATEGIES.contains(clazz)).forEach(builder::without);
        return builder;
    }
}