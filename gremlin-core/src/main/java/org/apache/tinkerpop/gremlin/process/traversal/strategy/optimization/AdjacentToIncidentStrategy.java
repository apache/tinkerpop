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
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.PropertyType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This strategy looks for vertex- and value-emitting steps followed by a {@link CountGlobalStep} and replaces the
 * pattern with an edge- or property-emitting step followed by a {@code CountGlobalStep}. Furthermore, if a vertex-
 * or value-emitting step is the last step in a {@code .has(traversal)}, {@code .and(traversal, ...)} or
 * {@code .or(traversal, ...)} child traversal, it is replaced by an appropriate edge- or property-emitting step.
 * <p/>
 *
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @example <pre>
 * __.out().count()            // is replaced by __.outE().count()
 * __.in().limit(3).count()    // is replaced by __.inE().limit(3).count()
 * __.values("name").count()   // is replaced by __.properties("name").count()
 * __.where(__.out())          // is replaced by __.where(__.outE())
 * __.where(__.values())       // is replaced by __.where(__.properties())
 * __.and(__.in(), __.out())   // is replaced by __.and(__.inE(), __.outE())
 * </pre>
 */
public final class AdjacentToIncidentStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy>
        implements TraversalStrategy.OptimizationStrategy {

    private static final AdjacentToIncidentStrategy INSTANCE = new AdjacentToIncidentStrategy();
    private static final Set<Class<? extends OptimizationStrategy>> PRIORS = new HashSet<>(Arrays.asList(IdentityRemovalStrategy.class, IncidentToAdjacentStrategy.class));

    private AdjacentToIncidentStrategy() {
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        final List<Step> steps = traversal.getSteps();
        final int size = steps.size() - 1;
        Step prev = null;
        for (int i = 0; i <= size; i++) {
            final Step curr = steps.get(i);
            if (i == size && isOptimizable(curr)) {
                final TraversalParent parent = curr.getTraversal().getParent();
                if (parent instanceof NotStep || parent instanceof TraversalFilterStep || parent instanceof WhereTraversalStep || parent instanceof ConnectiveStep) {
                    optimizeStep(traversal, curr);
                }
            } else if (isOptimizable(prev)) {
                if (curr instanceof CountGlobalStep) {
                    optimizeStep(traversal, prev);
                }
            }
            if (!(curr instanceof RangeGlobalStep)) {
                prev = curr;
            }
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return PRIORS;
    }

    /**
     * Checks whether a given step is optimizable or not.
     *
     * @param step the step to check
     * @return <code>true</code> if the step is optimizable, otherwise <code>false</code>
     */
    private static boolean isOptimizable(final Step step) {
        return ((step instanceof VertexStep && ((VertexStep) step).returnsVertex()) ||
                (step instanceof PropertiesStep && PropertyType.VALUE.equals(((PropertiesStep) step).getReturnType()))) && (step.getTraversal().getEndStep().getLabels().isEmpty() || step.getNextStep() instanceof CountGlobalStep);
    }

    /**
     * Optimizes the given step if possible. Basically this method converts <code>.out()</code> to <code>.outE()</code>
     * and <code>.values()</code> to <code>.properties()</code>.
     *
     * @param traversal the traversal that holds the given step
     * @param step      the step to replace
     */
    private static void optimizeStep(final Traversal.Admin traversal, final Step step) {
        final Step newStep;
        if (step instanceof VertexStep) {
            final VertexStep vs = (VertexStep) step;
            newStep = new VertexStep<>(traversal, Edge.class, vs.getDirection(), vs.getEdgeLabels());
        } else if (step instanceof PropertiesStep) {
            final PropertiesStep ps = (PropertiesStep) step;
            newStep = new PropertiesStep(traversal, PropertyType.PROPERTY, ps.getPropertyKeys());
        } else {
            return;
        }
        TraversalHelper.replaceStep(step, newStep, traversal);
    }

    public static AdjacentToIncidentStrategy instance() {
        return INSTANCE;
    }
}
