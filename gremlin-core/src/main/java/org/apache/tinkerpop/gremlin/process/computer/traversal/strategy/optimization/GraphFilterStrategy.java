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
import org.apache.tinkerpop.gremlin.process.computer.util.EmptyMemory;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
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
        if (TraversalHelper.getStepsOfAssignableClass(VertexProgramStep.class, traversal).size() > 1)  // do not do if there is an OLAP chain
            return;
        final Graph graph = traversal.getGraph().orElse(EmptyGraph.instance()); // given that this strategy only works for single OLAP jobs, the graph is the traversal graph
        for (final TraversalVertexProgramStep step : TraversalHelper.getStepsOfClass(TraversalVertexProgramStep.class, traversal)) {   // will be zero or one step
            final Traversal.Admin<?, ?> computerTraversal = step.generateProgram(EmptyMemory.instance(), graph).getTraversal().get().clone();
            if (!computerTraversal.isLocked())
                computerTraversal.applyStrategies();
            final Computer computer = step.getComputer();
            if (null == computer.getEdges() && !GraphComputer.Persist.EDGES.equals(computer.getPersist())) {  // if edges() already set, use it
                final Traversal.Admin<Vertex, Edge> edgeFilter = getEdgeFilter(computerTraversal);
                if (null != edgeFilter)  // if no edges can be filtered, then don't set edges()
                    step.setComputer(computer.edges(edgeFilter));
            }
        }
    }

    protected static Traversal.Admin<Vertex, Edge> getEdgeFilter(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getStartStep() instanceof GraphStep && ((GraphStep) traversal.getStartStep()).returnsEdge())
            return null; // if the traversal is an edge traversal, don't filter (this can be made less stringent)
        if (TraversalHelper.hasStepOfAssignableClassRecursively(LambdaHolder.class, traversal))
            return null; // if the traversal contains lambdas, don't filter as you don't know what is being accessed by the lambdas
        final Map<Direction, Set<String>> directionLabels = new HashMap<>();
        final Set<String> outLabels = new HashSet<>();
        final Set<String> inLabels = new HashSet<>();
        final Set<String> bothLabels = new HashSet<>();
        directionLabels.put(Direction.OUT, outLabels);
        directionLabels.put(Direction.IN, inLabels);
        directionLabels.put(Direction.BOTH, bothLabels);
        TraversalHelper.getStepsOfAssignableClassRecursively(VertexStep.class, traversal).forEach(step -> {
            // in-edge traversals require the outgoing edges for attachment
            final Direction direction = step.getDirection().equals(Direction.IN) && step.returnsEdge() ?
                    Direction.BOTH :
                    step.getDirection();
            final String[] edgeLabels = step.getEdgeLabels();
            if (edgeLabels.length == 0)
                directionLabels.get(direction).add(null); // null means all edges (don't filter)
            else
                Collections.addAll(directionLabels.get(direction), edgeLabels); // add edge labels associated with that direction
        });
        for (final String label : outLabels) { // if both in and out share the same labels, add them to both
            if (inLabels.contains(label)) {
                bothLabels.add(label);
            }
        }
        if (bothLabels.contains(null)) // if both on everything, you can't edges() filter
            return null;

        for (final String label : bothLabels) { // remove labels from out and in that are already handled by both
            outLabels.remove(label);
            inLabels.remove(label);
        }
        // construct edges(...)
        if (outLabels.isEmpty() && inLabels.isEmpty() && bothLabels.isEmpty())  // out/in/both are never called, thus, filter all edges
            return __.<Vertex>bothE().limit(0).asAdmin();
        else {
            final String[] ins = inLabels.contains(null) ? new String[]{} : inLabels.toArray(new String[inLabels.size()]);
            final String[] outs = outLabels.contains(null) ? new String[]{} : outLabels.toArray(new String[outLabels.size()]);
            final String[] boths = bothLabels.contains(null) ? new String[]{} : bothLabels.toArray(new String[bothLabels.size()]);

            if (outLabels.isEmpty() && inLabels.isEmpty()) // only both has labels
                return __.<Vertex>bothE(boths).asAdmin();
            else if (inLabels.isEmpty() && bothLabels.isEmpty()) // only out has labels
                return __.<Vertex>outE(outs).asAdmin();
            else if (outLabels.isEmpty() && bothLabels.isEmpty()) // only in has labels
                return __.<Vertex>inE(ins).asAdmin();
            else if (bothLabels.isEmpty())                        // out and in both have labels
                return __.<Vertex, Edge>union(__.inE(ins), __.outE(outs)).asAdmin();
            else if (outLabels.isEmpty() && ins.length > 0)       // in and both have labels (and in is not null)
                return __.<Vertex, Edge>union(__.inE(ins), __.bothE(boths)).asAdmin();
            else if (inLabels.isEmpty() && outs.length > 0)       // out and both have labels (and out is not null)
                return __.<Vertex, Edge>union(__.outE(outs), __.bothE(boths)).asAdmin();
            else
                return null;
            //throw new IllegalStateException("The label combination should not have reached this point: " + outLabels + "::" + inLabels + "::" + bothLabels);
        }
    }

    public static GraphFilterStrategy instance() {
        return INSTANCE;
    }
}
