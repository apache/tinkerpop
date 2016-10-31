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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LazyBarrierStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    protected static final int MAX_BARRIER_SIZE = 2500;
    private static final LazyBarrierStrategy INSTANCE = new LazyBarrierStrategy();
    private static final Set<Class<? extends OptimizationStrategy>> PRIORS = new HashSet<>(Arrays.asList(
            RangeByIsCountStrategy.class,
            PathRetractionStrategy.class,
            IncidentToAdjacentStrategy.class,
            AdjacentToIncidentStrategy.class,
            FilterRankingStrategy.class,
            InlineFilterStrategy.class,
            MatchPredicateStrategy.class));

    private static final int BIG_START_SIZE = 5;
    private final boolean IS_TESTING = Boolean.valueOf(System.getProperty("is.testing", "false"));

    private LazyBarrierStrategy() {
    }

    public static LazyBarrierStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal) ||
                traversal.getTraverserRequirements().contains(TraverserRequirement.PATH) ||
                (IS_TESTING && (TraversalHelper.hasStepOfAssignableClass(ProfileStep.class, TraversalHelper.getRootTraversal(traversal))) ||
                        TraversalHelper.hasStepOfAssignableClass(ProfileSideEffectStep.class, TraversalHelper.getRootTraversal(traversal)))) // necessary cause ProfileTest analyzes counts
            return;

        final Set<String> lazyStepIds = traversal.<Set<String>>getMetadata(NoBarrierStrategy.LAZY_STEPS_METADATA_KEY).orElse(Collections.emptySet());
        boolean foundFlatMap = false;
        boolean labeledPath = false;
        for (int i = 0; i < traversal.getSteps().size(); i++) {
            final Step<?, ?> step = traversal.getSteps().get(i);

            if (step instanceof PathProcessor && labeledPath) {
                final Set<String> keepLabels = ((PathProcessor) step).getKeepLabels();
                if (null != keepLabels && keepLabels.isEmpty()) // if no more path data, then start barrier'ing again
                    labeledPath = false;
            }
            if (step instanceof FlatMapStep &&
                    !(step instanceof VertexStep && ((VertexStep) step).returnsEdge()) ||
                    (step instanceof GraphStep &&
                            (i > 0 || ((GraphStep) step).getIds().length >= BIG_START_SIZE ||
                                    (((GraphStep) step).getIds().length == 0 && !(step.getNextStep() instanceof HasStep))))) {
                if (foundFlatMap && !labeledPath && !lazyStepIds.contains(step.getId()) &&
                        !(step.getNextStep() instanceof Barrier) &&
                        (!(step.getNextStep() instanceof EmptyStep) || step.getTraversal().getParent() instanceof EmptyStep)) {
                    final Step noOpBarrierStep = new NoOpBarrierStep<>(traversal, MAX_BARRIER_SIZE);
                    if (labeledPath = !step.getLabels().isEmpty()) {
                        TraversalHelper.copyLabels(step, noOpBarrierStep, true);
                    }
                    TraversalHelper.insertAfterStep(noOpBarrierStep, step, traversal);
                    i++;
                    continue;
                } else
                    foundFlatMap = true;
            }
            if (!step.getLabels().isEmpty())
                labeledPath = true;

        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return PRIORS;
    }
}
