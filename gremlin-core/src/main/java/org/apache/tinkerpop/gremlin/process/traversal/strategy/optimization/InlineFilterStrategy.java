/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.GraphFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * InlineFilterStrategy analyzes filter-steps with child traversals that themselves are pure filters.
 * If the child traversals are pure filters then the wrapping parent filter is not needed and thus, the
 * children can be "inlined."
 * <p/>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @example <pre>
 * __.filter(has("name","marko"))                                // is replaced by __.has("name","marko")
 * __.and(has("name"),has("age"))                                // is replaced by __.has("name").has("age")
 * __.and(filter(has("name","marko").has("age")),hasNot("blah")) // is replaced by __.has("name","marko").has("age").hasNot("blah")
 * </pre>
 */
public final class InlineFilterStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final InlineFilterStrategy INSTANCE = new InlineFilterStrategy();
    private static final Set<Class<? extends OptimizationStrategy>> POSTS = new HashSet<>(Arrays.asList(
            FilterRankingStrategy.class,
            GraphFilterStrategy.class));

    private InlineFilterStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        boolean changed = true; // recursively walk child traversals trying to inline them into the current traversal line.
        while (changed) {
            changed = false;
            for (final TraversalFilterStep<?> step : TraversalHelper.getStepsOfAssignableClass(TraversalFilterStep.class, traversal)) {
                final Traversal.Admin<?, ?> childTraversal = step.getLocalChildren().get(0);
                if (TraversalHelper.allStepsInstanceOf(childTraversal, FilterStep.class, true)) {
                    changed = true;
                    TraversalHelper.applySingleLevelStrategies(traversal, childTraversal, InlineFilterStrategy.class);
                    final Step<?, ?> finalStep = childTraversal.getEndStep();
                    TraversalHelper.insertTraversal((Step) step, childTraversal, traversal);
                    TraversalHelper.copyLabels(step, finalStep, false);
                    traversal.removeStep(step);
                }
            }
            for (final AndStep<?> step : TraversalHelper.getStepsOfAssignableClass(AndStep.class, traversal)) {
                if (!step.getLocalChildren().stream().filter(t -> !TraversalHelper.allStepsInstanceOf(t, FilterStep.class, true)).findAny().isPresent()) {
                    changed = true;
                    final List<Traversal.Admin<?, ?>> childTraversals = (List) step.getLocalChildren();
                    Step<?, ?> finalStep = null;
                    for (int i = childTraversals.size() - 1; i >= 0; i--) {
                        final Traversal.Admin<?, ?> childTraversal = childTraversals.get(i);
                        TraversalHelper.applySingleLevelStrategies(traversal, childTraversal, InlineFilterStrategy.class);
                        if (null == finalStep)
                            finalStep = childTraversal.getEndStep();
                        TraversalHelper.insertTraversal((Step) step, childTraversals.get(i), traversal);

                    }
                    if (null != finalStep) TraversalHelper.copyLabels(step, finalStep, false);
                    traversal.removeStep(step);
                }
            }
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPost() {
        return POSTS;
    }

    public static InlineFilterStrategy instance() {
        return INSTANCE;
    }
}