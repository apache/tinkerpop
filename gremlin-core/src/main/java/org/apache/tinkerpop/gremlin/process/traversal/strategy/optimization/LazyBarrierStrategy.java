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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NoneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * {@code LazyBarrierStrategy} is an OLTP-only strategy that automatically inserts a {@link NoOpBarrierStep} after every
 * {@link FlatMapStep} if neither path-tracking nor partial path-tracking is required, and the next step is not the
 * traversal's last step or a {@link Barrier}. {@link NoOpBarrierStep}s allow traversers to be bulked, thus this strategy
 * is meant to reduce memory requirements and improve the overall query performance.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @example <pre>
 * __.out().bothE().count()      // is replaced by __.out().barrier(2500).bothE().count()
 * __.both().both().valueMap()   // is replaced by __.both().barrier(2500).both().barrier(2500).valueMap()
 * </pre>
 */
public final class LazyBarrierStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    public static final String BARRIER_PLACEHOLDER = Graph.Hidden.hide("gremlin.lazyBarrier.position");
    public static final String BARRIER_COPY_LABELS = Graph.Hidden.hide("gremlin.lazyBarrier.copyLabels");
    private static final LazyBarrierStrategy INSTANCE = new LazyBarrierStrategy();
    private static final Set<Class<? extends OptimizationStrategy>> PRIORS = new HashSet<>(Arrays.asList(
            CountStrategy.class,
            PathRetractionStrategy.class,
            IncidentToAdjacentStrategy.class,
            AdjacentToIncidentStrategy.class,
            FilterRankingStrategy.class,
            InlineFilterStrategy.class,
            MatchPredicateStrategy.class,
            EarlyLimitStrategy.class,
            RepeatUnrollStrategy.class));

    private static final int BIG_START_SIZE = 5;
    protected static final int MAX_BARRIER_SIZE = 2500;

    private LazyBarrierStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // drop()/element() is a problem for bulked edge/meta properties because of Property equality changes in TINKERPOP-2318
        // which made it so that a Property is equal if the key/value is equal. as a result, they bulk together which
        // is fine for almost all cases except when you wish to drop the property.
        if (TraversalHelper.onGraphComputer(traversal) ||
                traversal.getTraverserRequirements().contains(TraverserRequirement.PATH) ||
                TraversalHelper.hasStepOfAssignableClass(DropStep.class, traversal)||
                TraversalHelper.hasStepOfAssignableClass(ElementStep.class, traversal))
            return;

        boolean foundFlatMap = false;
        boolean labeledPath = false;
        for (int i = 0; i < traversal.getSteps().size(); i++) {
            final Step<?, ?> step = traversal.getSteps().get(i);

            if (step.getLabels().contains(BARRIER_PLACEHOLDER)) {
                TraversalHelper.insertAfterStep(new NoOpBarrierStep<>(traversal, MAX_BARRIER_SIZE), step, traversal);
                step.removeLabel(BARRIER_PLACEHOLDER);
                if (step.getLabels().contains(BARRIER_COPY_LABELS)) {
                    step.removeLabel(BARRIER_COPY_LABELS);
                    TraversalHelper.copyLabels(step, step.getNextStep(), true);
                }
            }

            if (step instanceof PathProcessor) {
                final Set<String> keepLabels = ((PathProcessor) step).getKeepLabels();
                if (null != keepLabels && keepLabels.isEmpty()) // if no more path data, then start barrier'ing again
                    labeledPath = false;
            }

            if (step instanceof FlatMapStep &&
                    !(step instanceof VertexStep && ((VertexStep) step).returnsEdge()) ||
                    (step instanceof GraphStep &&
                            (i > 0 || ((GraphStep) step).getIds().length >= BIG_START_SIZE ||
                                    (((GraphStep) step).getIds().length == 0 && !(step.getNextStep() instanceof HasStep))))) {

                // NoneStep, EmptyStep signify the end of the traversal where no barriers are really going to be
                // helpful after that. ProfileSideEffectStep means the traversal had profile() called on it and if
                // we don't account for that a barrier will inject at the end of the traversal where it wouldn't
                // be otherwise. LazyBarrierStrategy executes before the finalization strategy of ProfileStrategy
                // so additionally injected ProfileSideEffectStep instances should not have effect here.
                if (foundFlatMap && !labeledPath &&
                        !(step.getNextStep() instanceof Barrier) &&
                        !(step.getNextStep() instanceof NoneStep) &&
                        !(step.getNextStep() instanceof EmptyStep) &&
                        !(step.getNextStep() instanceof ProfileSideEffectStep)) {
                    final Step noOpBarrierStep = new NoOpBarrierStep<>(traversal, MAX_BARRIER_SIZE);
                    TraversalHelper.copyLabels(step, noOpBarrierStep, true);
                    TraversalHelper.insertAfterStep(noOpBarrierStep, step, traversal);
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

    public static LazyBarrierStrategy instance() {
        return INSTANCE;
    }
}
