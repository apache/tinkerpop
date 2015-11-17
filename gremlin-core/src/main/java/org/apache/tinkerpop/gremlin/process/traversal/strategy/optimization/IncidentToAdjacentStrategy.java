/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This strategy looks for {@code .outE().inV()}, {@code .inE().outV()} and {@code .bothE().otherV()}
 * and replaces these step sequences with {@code .out()}, {@code .in()} or {@code .both()} respectively.
 * The strategy won't modify the traversal if:
 * <ul>
 * <li>the edge step is labeled</li>
 * <li>the traversal contains a {@code path} step</li>
 * <li>the traversal contains a lambda step</li>
 * </ul>
 * <p/>
 *
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @example <pre>
 * __.outE().inV()         // is replaced by __.out()
 * __.inE().outV()         // is replaced by __.in()
 * __.bothE().otherV()     // is replaced by __.both()
 * __.bothE().bothV()      // will not be modified
 * __.outE().inV().path()  // will not be modified
 * </pre>
 */
public final class IncidentToAdjacentStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy>
        implements TraversalStrategy.OptimizationStrategy {

    private static final IncidentToAdjacentStrategy INSTANCE = new IncidentToAdjacentStrategy();
    private static final Set<Class> InvalidatingStepClasses = new HashSet<Class>() {{
        add(PathStep.class);
        add(LambdaHolder.class);
    }};

    private IncidentToAdjacentStrategy() {
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        final Traversal.Admin root = TraversalHelper.getRootTraversal(traversal);
        if (TraversalHelper.hasStepOfAssignableClassRecursively(InvalidatingStepClasses, root))
            return;
        final Collection<Pair<VertexStep, Step>> stepsToReplace = new ArrayList<>();
        Step prev = null;
        for (final Step curr : traversal.getSteps()) {
            if (isOptimizable(prev, curr)) {
                stepsToReplace.add(Pair.with((VertexStep) prev, curr));
            }
            prev = curr;
        }
        if (!stepsToReplace.isEmpty()) {
            for (final Pair<VertexStep, Step> pair : stepsToReplace) {
                optimizeSteps(traversal, pair.getValue0(), pair.getValue1());
            }
        }
    }

    /**
     * Checks whether a given step is optimizable or not.
     *
     * @param step1 an edge-emitting step
     * @param step2 a vertex-emitting step
     * @return <code>true</code> if step1 is not labeled and emits edges and step2 emits vertices,
     * otherwise <code>false</code>
     */
    private static boolean isOptimizable(final Step step1, final Step step2) {
        if (step1 instanceof VertexStep && ((VertexStep) step1).returnsEdge() && step1.getLabels().isEmpty()) {
            final Direction step1Dir = ((VertexStep) step1).getDirection();
            if (step1Dir.equals(Direction.BOTH)) {
                return step2 instanceof EdgeOtherVertexStep;
            }
            return step2 instanceof EdgeVertexStep && ((EdgeVertexStep) step2).getDirection().equals(step1Dir.opposite());
        }
        return false;
    }

    /**
     * Optimizes the given edge-emitting step and the vertex-emitting step by replacing them with a single
     * vertex-emitting step.
     *
     * @param traversal the traversal that holds the given steps
     * @param step1     the edge-emitting step to replace
     * @param step2     the vertex-emitting step to replace
     */
    private static void optimizeSteps(final Traversal.Admin traversal, final VertexStep step1, final Step step2) {
        final Step newStep = new VertexStep(traversal, Vertex.class, step1.getDirection(), step1.getEdgeLabels());
        for (final String label : (Iterable<String>) step2.getLabels()) {
            newStep.addLabel(label);
        }
        TraversalHelper.replaceStep(step1, newStep, traversal);
        traversal.removeStep(step2);
    }

    public static IncidentToAdjacentStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Collections.singleton(IdentityRemovalStrategy.class);
    }
}
