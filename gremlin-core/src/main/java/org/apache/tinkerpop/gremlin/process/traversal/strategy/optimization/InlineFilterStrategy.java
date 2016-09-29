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
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
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
            for (final FilterStep<?> step : TraversalHelper.getStepsOfAssignableClass(FilterStep.class, traversal)) {
                if (step instanceof HasStep && InlineFilterStrategy.processHasStep((HasStep) step, traversal))
                    // has(a,b).has(c) --> has(a,b,c)
                    changed = true;
                else if (step instanceof TraversalFilterStep && InlineFilterStrategy.processTraversalFilterStep((TraversalFilterStep) step, traversal))
                    // filter(x.y) --> x.y
                    changed = true;
                else if (step instanceof OrStep && InlineFilterStrategy.processOrStep((OrStep) step, traversal))
                    // or(has(x,y),has(x,z)) --> has(x,y.or(z))
                    changed = true;
                else if (step instanceof AndStep && InlineFilterStrategy.processAndStep((AndStep) step, traversal))
                    // and(x,y) --> x.y
                    changed = true;
            }
            // match(as('a').has(key,value),...) --> as('a').has(key,value).match(...)
            if (traversal.getParent() instanceof EmptyStep) {
                for (final MatchStep<?, ?> step : TraversalHelper.getStepsOfClass(MatchStep.class, traversal)) {
                    if (InlineFilterStrategy.processMatchStep(step, traversal))
                        changed = true;
                }
            }
        }
    }

    ////////////////////////////
    ///////////////////////////

    private static final boolean processHasStep(final HasStep<?> step, final Traversal.Admin<?, ?> traversal) {
        if (step.getPreviousStep() instanceof HasStep) {
            final HasStep<?> previousStep = (HasStep<?>) step.getPreviousStep();
            for (final HasContainer hasContainer : step.getHasContainers()) {
                previousStep.addHasContainer(hasContainer);
            }
            TraversalHelper.copyLabels(step, previousStep, false);
            traversal.removeStep(step);
            return true;
        }
        return false;
    }

    private static final boolean processTraversalFilterStep(final TraversalFilterStep<?> step, final Traversal.Admin<?, ?> traversal) {
        final Traversal.Admin<?, ?> childTraversal = step.getLocalChildren().get(0);
        if (TraversalHelper.hasAllStepsOfClass(childTraversal, FilterStep.class) &&
                !TraversalHelper.hasStepOfClass(childTraversal,
                        DropStep.class,
                        RangeGlobalStep.class,
                        DedupGlobalStep.class,
                        LambdaHolder.class)) {
            TraversalHelper.applySingleLevelStrategies(traversal, childTraversal, InlineFilterStrategy.class);
            final Step<?, ?> finalStep = childTraversal.getEndStep();
            TraversalHelper.insertTraversal((Step) step, childTraversal, traversal);
            TraversalHelper.copyLabels(step, finalStep, false);
            traversal.removeStep(step);
            return true;
        }
        return false;
    }

    private static final boolean processOrStep(final OrStep<?> step, final Traversal.Admin<?, ?> traversal) {
        boolean process = true;
        String key = null;
        P predicate = null;
        final List<String> labels = new ArrayList<>();
        for (final Traversal.Admin<?, ?> childTraversal : step.getLocalChildren()) {
            InlineFilterStrategy.instance().apply(childTraversal); // todo: this may be a bad idea, but I can't seem to find a test case to break it
            for (final Step<?, ?> childStep : childTraversal.getSteps()) {
                if (childStep instanceof HasStep) {
                    for (final HasContainer hasContainer : ((HasStep<?>) childStep).getHasContainers()) {
                        if (null == key)
                            key = hasContainer.getKey();
                        else if (!hasContainer.getKey().equals(key)) {
                            process = false;
                            break;
                        }
                        predicate = null == predicate ?
                                hasContainer.getPredicate() :
                                predicate.or(hasContainer.getPredicate());
                    }
                    labels.addAll(childStep.getLabels());
                } else {
                    process = false;
                    break;
                }
            }
            if (!process)
                break;
        }
        if (process) {
            final HasStep hasStep = new HasStep<>(traversal, new HasContainer(key, predicate));
            TraversalHelper.replaceStep(step, hasStep, traversal);
            TraversalHelper.copyLabels(step, hasStep, false);
            for (final String label : labels) {
                hasStep.addLabel(label);
            }
            return true;
        }
        return false;
    }

    private static final boolean processAndStep(final AndStep<?> step, final Traversal.Admin<?, ?> traversal) {
        boolean process = true;
        for (final Traversal.Admin<?, ?> childTraversal : step.getLocalChildren()) {
            if (!TraversalHelper.hasAllStepsOfClass(childTraversal, FilterStep.class) ||
                    TraversalHelper.hasStepOfClass(childTraversal,
                            DropStep.class,
                            RangeGlobalStep.class,
                            DedupGlobalStep.class,
                            LambdaHolder.class)) {
                process = false;
                break;
            }
        }
        if (process) {
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
            return true;
        }
        return false;
    }

    private static final boolean processMatchStep(final MatchStep<?, ?> step, final Traversal.Admin<?, ?> traversal) {
        if (step.getPreviousStep() instanceof EmptyStep)
            return false;
        boolean changed = false;
        final String startLabel = MatchStep.Helper.computeStartLabel(step.getGlobalChildren());
        for (final Traversal.Admin<?, ?> matchTraversal : new ArrayList<>(step.getGlobalChildren())) {
            if (TraversalHelper.hasAllStepsOfClass(matchTraversal,
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
                // TODO: matchTraversal.getEndStep().addLabel(startLabel); (perhaps insert an identity so filter rank can push has()-left)
                if (null != endLabel) matchTraversal.getEndStep().addLabel(endLabel);
                TraversalHelper.insertTraversal((Step) step.getPreviousStep(), matchTraversal, traversal);
            }
        }
        if (step.getGlobalChildren().isEmpty())
            traversal.removeStep(step);
        return changed;
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPost() {
        return POSTS;
    }

    public static InlineFilterStrategy instance() {
        return INSTANCE;
    }
}