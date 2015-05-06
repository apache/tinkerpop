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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * This strategy looks for vertex- and value-emitting steps followed by a {@link CountGlobalStep} and replaces the
 * pattern with an edge- or property-emitting step followed by a <code>CountGlobalStep</code>. For example:
 * <p/>
 * <code>
 * __.out().count()          // is replaced by __.outE().count()
 * __.in().limit(3).count()  // is replaced by __.inE().limit(3).count()
 * __.values("name").count() // is replaced by __.properties("name").count()
 * </code>
 * <p/>
 * Furthermore, if a vertex- or value-emitting step is the last step in a <code>.has(traversal)</code> child traversal,
 * it is replaced by an appropriate edge- or property-emitting step. For example:
 * <p/>
 * <code>
 * __.has(out())    // is replaced by __.has(__.outE())
 * __.has(values()) // is replaced by __.has(__.properties())
 * </code>
 *
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class AdjacentToIncidentStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy>
        implements TraversalStrategy.OptimizationStrategy {

    private static final AdjacentToIncidentStrategy INSTANCE = new AdjacentToIncidentStrategy();

    private AdjacentToIncidentStrategy() {
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        final int size = traversal.getSteps().size() - 1;
        Step prev = null;
        for (int i = 0; i <= size; i++) {
            final Step curr = traversal.getSteps().get(i);
            if (i == size && isOptimizable(curr)) {
                final TraversalParent parent = curr.getTraversal().getParent();
                if (parent instanceof HasTraversalStep) {
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

    /**
     * Checks whether a given step is optimizable or not.
     *
     * @param step the step to check
     * @return <code>true</code> if the step is optimizable, otherwise <code>false</code>
     */
    private static boolean isOptimizable(final Step step) {
        return ((step instanceof VertexStep && Vertex.class.equals(((VertexStep) step).getReturnClass())) ||
                (step instanceof PropertiesStep && PropertyType.VALUE.equals(((PropertiesStep) step).getReturnType())));
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
