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

package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NoneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

/**
 * This strategy looks for {@link RangeGlobalStep}s that can be moved further left in the traversal and thus be applied
 * earlier. It will also try to merge multiple {@link RangeGlobalStep}s into one.
 * If the logical consequence of one or multiple {@link RangeGlobalStep}s is an empty result, the strategy will remove
 * as many steps as possible and add a {@link NoneStep} instead.
 *
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @example <pre>
 * __.out().valueMap().limit(5)                          // becomes __.out().limit(5).valueMap()
 * __.outE().range(2, 10).valueMap().limit(5)            // becomes __.outE().range(2, 7).valueMap()
 * __.outE().limit(5).valueMap().range(2, -1)            // becomes __.outE().range(2, 5).valueMap()
 * __.outE().limit(5).valueMap().range(5, 10)            // becomes __.outE().none()
 * __.outE().limit(5).valueMap().range(5, 10).cap("a")   // becomes __.outE().none().cap("a")
 * </pre>
 */
public final class EarlyLimitStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy>
        implements TraversalStrategy.OptimizationStrategy {

    private static final EarlyLimitStrategy INSTANCE = new EarlyLimitStrategy();

    private EarlyLimitStrategy() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {

        final List<Step> steps = traversal.getSteps();
        Step insertAfter = null;
        boolean merge = false;
        for (int i = 0, j = steps.size(); i < j; i++) {
            final Step step = steps.get(i);
            if (step instanceof RangeGlobalStep) {
                if (insertAfter != null) {
                    // RangeStep was found, move it to the earliest possible step or merge it with a
                    // previous RangeStep; keep the RangeStep's labels at its preceding step
                    TraversalHelper.copyLabels(step, step.getPreviousStep(), true);
                    insertAfter = moveRangeStep((RangeGlobalStep) step, insertAfter, traversal, merge);
                    if (insertAfter instanceof NoneStep) {
                        // any step besides a SideEffectCapStep after a NoneStep would be pointless
                        final int noneStepIndex = TraversalHelper.stepIndex(insertAfter, traversal);
                        for (i = j - 2; i > noneStepIndex; i--) {
                            if (!(steps.get(i) instanceof SideEffectCapStep) && !(steps.get(i) instanceof ProfileSideEffectStep)) {
                                traversal.removeStep(i);
                            }
                        }
                        break;
                    }
                    j = steps.size();
                }
            } else if (!(step instanceof MapStep || step instanceof SideEffectStep)) {
                // remember the last step that can be used to move any RangeStep to
                // any RangeStep can be moved in front of all its preceding map- and sideEffect-steps
                insertAfter = step;
                merge = true;
            } else if (step instanceof SideEffectCapable) {
                // if there's any SideEffectCapable step along the way, RangeSteps cannot be merged as this could
                // change the final traversal's internal memory
                merge = false;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Step moveRangeStep(final RangeGlobalStep step, final Step insertAfter, final Traversal.Admin<?, ?> traversal,
                               final boolean merge) {
        final Step rangeStep;
        boolean remove = true;
        if (insertAfter instanceof RangeGlobalStep) {
            // there's a previous RangeStep which might affect the effective range of the current RangeStep
            // recompute this step's low and high; if the result is still a valid range, create a new RangeStep,
            // otherwise a NoneStep
            final RangeGlobalStep other = (RangeGlobalStep) insertAfter;
            final long low = other.getLowRange() + step.getLowRange();
            if (other.getHighRange() == -1L) {
                rangeStep = new RangeGlobalStep(traversal, low, other.getLowRange() + step.getHighRange());
            } else if (step.getHighRange() == -1L) {
                final long high = other.getHighRange() - other.getLowRange() - step.getLowRange() + low;
                if (low < high) {
                    rangeStep = new RangeGlobalStep(traversal, low, high);
                } else {
                    rangeStep = new NoneStep<>(traversal);
                }
            } else {
                final long high = Math.min(other.getLowRange() + step.getHighRange(), other.getHighRange());
                rangeStep = high > low ? new RangeGlobalStep(traversal, low, high) : new NoneStep<>(traversal);
            }
            remove = merge;
            TraversalHelper.replaceStep(merge ? insertAfter : step, rangeStep, traversal);
        } else if (!step.getPreviousStep().equals(insertAfter, true)) {
            // move the RangeStep behind the earliest possible map- or sideEffect-step
            rangeStep = step.clone();
            TraversalHelper.insertAfterStep(rangeStep, insertAfter, traversal);
        } else {
            // no change if the earliest possible step to insert the RangeStep after is
            // already the current step's previous step
            return step;
        }
        if (remove) traversal.removeStep(step);
        return rangeStep;
    }

    public static EarlyLimitStrategy instance() {
        return INSTANCE;
    }
}
