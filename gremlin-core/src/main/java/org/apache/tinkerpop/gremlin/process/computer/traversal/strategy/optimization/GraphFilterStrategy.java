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

package org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GraphFilterStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final GraphFilterStrategy INSTANCE = new GraphFilterStrategy();

    private GraphFilterStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.getStepsOfAssignableClass(VertexProgramStep.class, traversal).size() > 1)
            return;
        final Graph graph = traversal.getGraph().orElse(EmptyGraph.instance()); // best guess at what the graph will be as its dynamically determined
        for (final TraversalVertexProgramStep step : TraversalHelper.getStepsOfClass(TraversalVertexProgramStep.class, traversal)) {
            final Traversal.Admin<?, ?> computerTraversal = step.generateProgram(graph).getTraversal().get().clone();
            if (!computerTraversal.isLocked())
                computerTraversal.applyStrategies();
            final Computer computer = step.getComputer();
            if (null == computer.getEdges() && !GraphComputer.Persist.EDGES.equals(computer.getPersist())) {
                final Traversal.Admin<Vertex, Edge> edgeFilter = getEdgeFilter(computerTraversal);
                if (null != edgeFilter)
                    step.setComputer(computer.edges(edgeFilter));
            }
        }
    }

    protected static Traversal.Admin<Vertex, Edge> getEdgeFilter(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getStartStep() instanceof GraphStep && ((GraphStep) traversal.getStartStep()).returnsEdge())
            return null;
        final Map<Direction, Set<String>> directionLabels = new HashMap<>();
        final Set<String> outLabels = new HashSet<>();
        final Set<String> inLabels = new HashSet<>();
        final Set<String> bothLabels = new HashSet<>();
        directionLabels.put(Direction.OUT, outLabels);
        directionLabels.put(Direction.IN, inLabels);
        directionLabels.put(Direction.BOTH, bothLabels);
        TraversalHelper.getStepsOfAssignableClassRecursively(VertexStep.class, traversal).forEach(step -> {
            // in-edge traversals require the outgoing edges for attachement
            final Direction direction = step.getDirection().equals(Direction.IN) && step.returnsEdge() ?
                    Direction.BOTH :
                    step.getDirection();
            final String[] edgeLabels = step.getEdgeLabels();
            Set<String> temp = directionLabels.get(direction);
            if (edgeLabels.length == 0)
                temp.add(null);
            else
                Collections.addAll(temp, edgeLabels);
        });
        for (final String label : outLabels) {
            if (inLabels.contains(label)) {
                if (null == label)
                    bothLabels.clear();
                if (!bothLabels.contains(null))
                    bothLabels.add(label);
            }
        }
        if (bothLabels.contains(null)) {
            outLabels.clear();
            inLabels.clear();
        }
        for (final String label : bothLabels) {
            outLabels.remove(label);
            inLabels.remove(label);
        }
        // construct edges(...)
        if (bothLabels.contains(null))
            return null;
        else if (outLabels.isEmpty() && inLabels.isEmpty() && bothLabels.isEmpty())
            return __.<Vertex>bothE().limit(0).asAdmin();
        else {
            final String[] ins = inLabels.contains(null) ? new String[]{} : inLabels.toArray(new String[inLabels.size()]);
            final String[] outs = outLabels.contains(null) ? new String[]{} : outLabels.toArray(new String[outLabels.size()]);
            final String[] boths = bothLabels.contains(null) ? new String[]{} : bothLabels.toArray(new String[bothLabels.size()]);

            if (outLabels.isEmpty() && inLabels.isEmpty())
                return __.<Vertex>bothE(boths).asAdmin();
            else if (inLabels.isEmpty() && bothLabels.isEmpty())
                return __.<Vertex>outE(outs).asAdmin();
            else if (outLabels.isEmpty() && bothLabels.isEmpty())
                return __.<Vertex>inE(ins).asAdmin();
            else if (outLabels.isEmpty())
                return __.<Vertex, Edge>union(__.inE(ins), __.bothE(boths)).asAdmin();
            else if (inLabels.isEmpty())
                return __.<Vertex, Edge>union(__.outE(outs), __.bothE(boths)).asAdmin();
            else if (bothLabels.isEmpty())
                return __.<Vertex, Edge>union(__.inE(ins), __.outE(outs)).asAdmin();
            else
                return null;  // no filter
        }
    }

    public static GraphFilterStrategy instance() {
        return INSTANCE;
    }
}
