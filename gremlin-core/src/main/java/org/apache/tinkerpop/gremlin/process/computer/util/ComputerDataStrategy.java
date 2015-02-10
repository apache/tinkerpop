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
package org.apache.tinkerpop.gremlin.process.computer.util;

import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.strategy.GraphStrategy;
import org.apache.tinkerpop.gremlin.structure.strategy.StrategyContext;
import org.apache.tinkerpop.gremlin.structure.strategy.StrategyGraph;
import org.apache.tinkerpop.gremlin.structure.strategy.StrategyVertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ComputerDataStrategy implements GraphStrategy {

    private final Set<String> elementComputeKeys;

    public ComputerDataStrategy(final Set<String> elementComputeKeys) {
        this.elementComputeKeys = elementComputeKeys;
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<VertexProperty<V>>>> getVertexIteratorsPropertyIteratorStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy strategyComposer) {
        return (f) -> (keys) -> keys.length == 0 ? IteratorUtils.filter(f.apply(keys), property -> !this.elementComputeKeys.contains(property.key())) : f.apply(keys);
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<V>>> getVertexIteratorsValueIteratorStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy strategyComposer) {
        return (f) -> (keys) -> IteratorUtils.map(ctx.getCurrent().iterators().<V>propertyIterator(keys), vertexProperty -> vertexProperty.value());
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getVertexKeysStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy strategyComposer) {
        return (f) -> () -> IteratorUtils.fill(IteratorUtils.filter(f.get().iterator(), key -> !this.elementComputeKeys.contains(key)), new HashSet<>());
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyString(this);
    }

    public static StrategyGraph wrapGraph(final Graph graph, final VertexProgram<?> vertexProgram) {
        return graph.strategy(new ComputerDataStrategy(vertexProgram.getElementComputeKeys()));
    }

    public static StrategyVertex wrapVertex(final Vertex vertex, final VertexProgram<?> vertexProgram) {
        return new StrategyVertex(vertex, vertex.graph().strategy(new ComputerDataStrategy(vertexProgram.getElementComputeKeys())));
    }

}
