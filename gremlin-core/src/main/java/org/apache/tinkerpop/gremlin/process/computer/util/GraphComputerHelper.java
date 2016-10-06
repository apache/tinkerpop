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

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GraphComputerHelper {

    private GraphComputerHelper() {
    }

    public static void validateProgramOnComputer(final GraphComputer computer, final VertexProgram vertexProgram) {
        if (vertexProgram.getMemoryComputeKeys().contains(null))
            throw Memory.Exceptions.memoryKeyCanNotBeNull();
        if (vertexProgram.getMemoryComputeKeys().contains(""))
            throw Memory.Exceptions.memoryKeyCanNotBeEmpty();

        final GraphComputer.Features graphComputerFeatures = computer.features();
        final VertexProgram.Features vertexProgramFeatures = vertexProgram.getFeatures();

        for (final Method method : VertexProgram.Features.class.getMethods()) {
            if (method.getName().startsWith("requires")) {
                final boolean supports;
                final boolean requires;
                try {
                    supports = (boolean) GraphComputer.Features.class.getMethod(method.getName().replace("requires", "supports")).invoke(graphComputerFeatures);
                    requires = (boolean) method.invoke(vertexProgramFeatures);
                } catch (final Exception e) {
                    throw new IllegalStateException("A reflection exception has occurred: " + e.getMessage(), e);
                }
                if (requires && !supports)
                    throw new IllegalStateException("The vertex program can not be executed on the graph computer: " + method.getName());
            }
        }
    }

    public static GraphComputer.ResultGraph getResultGraphState(final Optional<VertexProgram> vertexProgram, final Optional<GraphComputer.ResultGraph> resultGraph) {
        return resultGraph.isPresent() ? resultGraph.get() : vertexProgram.isPresent() ? vertexProgram.get().getPreferredResultGraph() : GraphComputer.ResultGraph.ORIGINAL;
    }

    public static GraphComputer.Persist getPersistState(final Optional<VertexProgram> vertexProgram, final Optional<GraphComputer.Persist> persist) {
        return persist.isPresent() ? persist.get() : vertexProgram.isPresent() ? vertexProgram.get().getPreferredPersist() : GraphComputer.Persist.NOTHING;
    }

    public static boolean areEqual(final MapReduce a, final Object b) {
        if (null == a)
            throw Graph.Exceptions.argumentCanNotBeNull("a");
        if (null == b)
            throw Graph.Exceptions.argumentCanNotBeNull("b");

        if (!(b instanceof MapReduce)) return false;
        return a.getClass().equals(b.getClass()) && a.getMemoryKey().equals(((MapReduce) b).getMemoryKey());
    }

    public static TraversalStrategy[] getTraversalStrategies(final TraversalSource traversalSource, final Computer computer) {
        Class<? extends GraphComputer> graphComputerClass;
        if (computer.getGraphComputerClass().equals(GraphComputer.class)) {
            try {
                graphComputerClass = computer.apply(traversalSource.getGraph()).getClass();
            } catch (final Exception e) {
                graphComputerClass = GraphComputer.class;
            }
        } else
            graphComputerClass = computer.getGraphComputerClass();
        final List<TraversalStrategy<?>> graphComputerStrategies = TraversalStrategies.GlobalCache.getStrategies(graphComputerClass).toList();
        final TraversalStrategy[] traversalStrategies = new TraversalStrategy[graphComputerStrategies.size() + 1];
        traversalStrategies[0] = new VertexProgramStrategy(computer);
        for (int i = 0; i < graphComputerStrategies.size(); i++) {
            traversalStrategies[i + 1] = graphComputerStrategies.get(i);
        }
        return traversalStrategies;
    }
}
