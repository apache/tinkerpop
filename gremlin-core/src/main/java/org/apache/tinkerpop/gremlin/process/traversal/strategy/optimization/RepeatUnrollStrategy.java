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

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * {@code RepeatUnrollStrategy} is an OLTP-only strategy that unrolls any {@link RepeatStep} if it uses a constant
 * number of loops ({@code times(x)}) and doesn't emit intermittent elements. If any of the following 3 steps appears
 * within the repeat-traversal, the strategy will not be applied:
 * <p/>
 * <ul>
 *     <li>{@link DedupGlobalStep}</li>
 *     <li>{@link LoopsStep}</li>
 *     <li>{@link LambdaHolder}</li>
 * </ul>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @example <pre>
 *     __.repeat(out()).times(2)   // is replaced by __.out().barrier(2500).out().barrier(2500)
 * </pre>
 */
public final class RepeatUnrollStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final RepeatUnrollStrategy INSTANCE = new RepeatUnrollStrategy();
    protected static final int MAX_BARRIER_SIZE = 2500;
    private static final Set<Class> INVALIDATING_STEPS = new HashSet<>(Arrays.asList(LambdaHolder.class, LoopsStep.class));

    private RepeatUnrollStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        final boolean lazyBarrierStrategyInstalled = TraversalHelper.getRootTraversal(traversal).
                getStrategies().getStrategy(LazyBarrierStrategy.class).isPresent();

        for (int i = 0; i < traversal.getSteps().size(); i++) {
            if (traversal.getSteps().get(i) instanceof RepeatStep) {
                final RepeatStep<?> repeatStep = (RepeatStep) traversal.getSteps().get(i);
                if (null == repeatStep.getEmitTraversal() && null != repeatStep.getRepeatTraversal() &&
                        repeatStep.getUntilTraversal() instanceof LoopTraversal && ((LoopTraversal) repeatStep.getUntilTraversal()).getMaxLoops() > 0 &&
                        !TraversalHelper.hasStepOfAssignableClassRecursively(Scope.global, DedupGlobalStep.class, repeatStep.getRepeatTraversal()) &&
                        !TraversalHelper.hasStepOfAssignableClassRecursively(INVALIDATING_STEPS, repeatStep.getRepeatTraversal())) {
                    final Traversal.Admin<?, ?> repeatTraversal = repeatStep.getGlobalChildren().get(0);
                    repeatTraversal.removeStep(repeatTraversal.getSteps().size() - 1); // removes the RepeatEndStep
                    TraversalHelper.applySingleLevelStrategies(traversal, repeatTraversal, RepeatUnrollStrategy.class);
                    final int repeatLength = repeatTraversal.getSteps().size();
                    int insertIndex = i;
                    final int loops = (int) ((LoopTraversal) repeatStep.getUntilTraversal()).getMaxLoops();
                    for (int j = 0; j < loops; j++) {
                        TraversalHelper.insertTraversal(insertIndex, repeatTraversal.clone(), traversal);
                        insertIndex = insertIndex + repeatLength;

                        // the addition of barriers is determined by the existence of LazyBarrierStrategy
                        if (lazyBarrierStrategyInstalled) {
                            // only add a final NoOpBarrier is subsequent step is not a barrier
                            // Don't add a barrier if this step is a barrier (prevents nested repeat adding the barrier multiple times)
                            Step step = traversal.getSteps().get(insertIndex);
                            if ((j != (loops - 1) || !(step.getNextStep() instanceof Barrier)) && !(step instanceof NoOpBarrierStep)) {
                                traversal.addStep(++insertIndex, new NoOpBarrierStep<>(traversal, MAX_BARRIER_SIZE));
                            }
                        }
                    }

                    // label last step if repeat() was labeled
                    if (!repeatStep.getLabels().isEmpty())
                        TraversalHelper.copyLabels(repeatStep, traversal.getSteps().get(insertIndex), false);

                    // remove the RepeatStep
                    traversal.removeStep(i);
                }
            }
        }
    }


    public static RepeatUnrollStrategy instance() {
        return INSTANCE;
    }
}
