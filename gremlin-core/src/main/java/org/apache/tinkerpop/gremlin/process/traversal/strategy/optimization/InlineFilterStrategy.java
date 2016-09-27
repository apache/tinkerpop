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
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.ArrayList;
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
 * __.match(as('a').has(key,value),...)                          // is replaced by __.as('a').has(key,value).match(...)
 * </pre>
 */
public final class InlineFilterStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final InlineFilterStrategy INSTANCE = new InlineFilterStrategy();
    private static final Set<Class<? extends OptimizationStrategy>> POSTS = new HashSet<>(Arrays.asList(
            MatchPredicateStrategy.class,
            FilterRankingStrategy.class,
            GraphFilterStrategy.class));

    private InlineFilterStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        boolean changed = true; // recursively walk child traversals trying to inline them into the current traversal line.
        while (changed) {
            changed = false;
            // filter(x.y) --> x.y
            for (final TraversalFilterStep<?> step : TraversalHelper.getStepsOfClass(TraversalFilterStep.class, traversal)) {
                final Traversal.Admin<?, ?> childTraversal = step.getLocalChildren().get(0);
                if (TraversalHelper.hasAllStepsOfClass(childTraversal, FilterStep.class) &&
                        !TraversalHelper.hasStepOfClass(childTraversal,
                                DropStep.class,
                                RangeGlobalStep.class,
                                TailGlobalStep.class,
                                DedupGlobalStep.class,
                                LambdaHolder.class)) {
                    changed = true;
                    TraversalHelper.applySingleLevelStrategies(traversal, childTraversal, InlineFilterStrategy.class);
                    final Step<?, ?> finalStep = childTraversal.getEndStep();
                    TraversalHelper.insertTraversal((Step) step, childTraversal, traversal);
                    TraversalHelper.copyLabels(step, finalStep, false);
                    traversal.removeStep(step);
                }
            }
            // and(x,y) --> x.y
            for (final AndStep<?> step : TraversalHelper.getStepsOfClass(AndStep.class, traversal)) {
                boolean process = true;
                for (final Traversal.Admin<?, ?> childTraversal : step.getLocalChildren()) {
                    if (!TraversalHelper.hasAllStepsOfClass(childTraversal, FilterStep.class) ||
                            TraversalHelper.hasStepOfClass(childTraversal,
                                    DropStep.class,
                                    RangeGlobalStep.class,
                                    TailGlobalStep.class,
                                    DedupGlobalStep.class,
                                    LambdaHolder.class)) {
                        process = false;
                        break;
                    }
                }
                if (process) {
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
            // match(as('a').has(key,value),...) --> as('a').has(key,value).match(...)
            if (traversal.getParent() instanceof EmptyStep) {
                for (final MatchStep<?, ?> step : TraversalHelper.getStepsOfClass(MatchStep.class, traversal)) {
                    final String startLabel = determineStartLabelForHasPullOut(step);
                    if (null != startLabel) {
                        for (final Traversal.Admin<?, ?> matchTraversal : new ArrayList<>(step.getGlobalChildren())) {
                            if (!(step.getPreviousStep() instanceof EmptyStep) &&
                                    TraversalHelper.hasAllStepsOfClass(matchTraversal,
                                            HasStep.class,
                                            MatchStep.MatchStartStep.class,
                                            MatchStep.MatchEndStep.class) &&
                                    matchTraversal.getStartStep() instanceof MatchStep.MatchStartStep &&
                                    startLabel.equals(((MatchStep.MatchStartStep) matchTraversal.getStartStep()).getSelectKey().orElse(null))) {
                                changed = true;
                                final String endLabel = ((MatchStep.MatchEndStep) matchTraversal.getEndStep()).getMatchKey().orElse(null); // why would this exist? but just in case
                                matchTraversal.removeStep(0);                                       // remove MatchStartStep
                                matchTraversal.removeStep(matchTraversal.getSteps().size() - 1);    // remove MatchEndStep
                                TraversalHelper.applySingleLevelStrategies(traversal, matchTraversal, InlineFilterStrategy.class);
                                step.removeGlobalChild(matchTraversal);
                                step.getPreviousStep().addLabel(startLabel);
                                if (null != endLabel) matchTraversal.getEndStep().addLabel(endLabel);
                                TraversalHelper.insertTraversal((Step) step.getPreviousStep(), matchTraversal, traversal);
                            }
                        }
                        if (step.getGlobalChildren().isEmpty())
                            traversal.removeStep(step);
                    }
                }
            }
        }

    }

    private static final String determineStartLabelForHasPullOut(final MatchStep<?, ?> matchStep) {
        final String startLabel = MatchStep.Helper.computeStartLabel(matchStep.getGlobalChildren());
        Step<?, ?> previousStep = matchStep.getPreviousStep();
        if (previousStep.getLabels().contains(startLabel))
            return startLabel;
        while (!(previousStep instanceof EmptyStep)) {
            if (!previousStep.getLabels().isEmpty())
                return null;
            previousStep = previousStep.getPreviousStep();
        }
        return startLabel;
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPost() {
        return POSTS;
    }

    public static InlineFilterStrategy instance() {
        return INSTANCE;
    }
}