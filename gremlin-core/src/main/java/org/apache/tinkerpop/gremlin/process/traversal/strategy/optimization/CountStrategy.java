/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.NotP;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TraversalSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * This strategy optimizes any occurrence of {@link CountGlobalStep} followed by an {@link IsStep}. The idea is to limit
 * the number of incoming elements in a way that it's enough for the {@link IsStep} to decide whether it evaluates
 * {@code true} or {@code false}. If the traversal already contains a user supplied limit, the strategy won't
 * modify it.
 *
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @example <pre>
 * __.outE().count().is(0)      // is replaced by __.not(outE())
 * __.outE().count().is(lt(3))  // is replaced by __.outE().limit(3).count().is(lt(3))
 * __.where(outE().count().is(gt(3)))  // is replaced by __.where(outE().limit(4).count().is(gt(3)))
 * __.outE().count().is(gt(3))  // is not changed as the real count is returned when it is greater than 3
 * </pre>
 */
public final class CountStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final Map<BiPredicate, Long> RANGE_PREDICATES = new HashMap<BiPredicate, Long>() {{
        put(Contains.within, 1L);
        put(Contains.without, 0L);
    }};
    private static final Set<PBiPredicate> INCREASED_OFFSET_SCALAR_PREDICATES =
            Set.of(Compare.eq, Compare.neq, Compare.lte, Compare.gt, new NotP.NotPBiPredicate(Compare.eq), new NotP.NotPBiPredicate(Compare.neq), new NotP.NotPBiPredicate(Compare.lt), new NotP.NotPBiPredicate(Compare.gt));

    private static final CountStrategy INSTANCE = new CountStrategy();

    private CountStrategy() {
    }

    public static CountStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final TraversalParent parent = traversal.getParent();
        int size = traversal.getSteps().size();
        Step prev = null;
        for (int i = 0; i < size; i++) {
            final Step curr = traversal.getSteps().get(i);
            if (i < size - 1 && doStrategy(curr)) {
                final IsStepContract isStep = (IsStepContract) traversal.getSteps().get(i + 1);
                final P isStepPredicate = isStep.getPredicate();
                Long highRange = null;
                boolean useNotStep = false, dismissCountIs = false, hasGtOrLtNegative = false;
                for (P p : isStepPredicate instanceof ConnectiveP ? ((ConnectiveP<?>) isStepPredicate).getPredicates() : Collections.singletonList(isStepPredicate)) {
                    final Object value = p.getValue();
                    final BiPredicate predicate = p.getBiPredicate();
                    if (value instanceof Number) {
                        final long highRangeOffset = INCREASED_OFFSET_SCALAR_PREDICATES.contains(predicate) ? 1L : 0L;
                        final Long highRangeCandidate = (long) Math.ceil(((Number) value).doubleValue()) + highRangeOffset;

                        if ((predicate.equals(Compare.gt) || predicate.equals(Compare.gte) || predicate.equals(Compare.lt) || predicate.equals(Compare.lte)) &&
                            highRangeCandidate < 1) {
                                hasGtOrLtNegative = true;
                        }

                        final boolean update = highRange == null || highRangeCandidate > highRange;
                        if (update) {
                            if (parent instanceof EmptyStep) {
                                useNotStep = false;
                            } else {
                                if (parent instanceof RepeatStep) {
                                    final RepeatStep repeatStep = (RepeatStep) parent;
                                    dismissCountIs = useNotStep = Objects.equals(traversal, repeatStep.getUntilTraversal())
                                            || Objects.equals(traversal, repeatStep.getEmitTraversal());
                                } else {
                                    dismissCountIs = useNotStep = parent instanceof FilterStep || parent instanceof SideEffectStep;
                                }
                            }
                            highRange = highRangeCandidate;
                            useNotStep &= curr.getLabels().isEmpty() && isStep.getLabels().isEmpty()
                                    && isStep.getNextStep() instanceof EmptyStep
                                    && ((highRange <= 1L && predicate.equals(Compare.lt))
                                    || (highRange == 1L && (predicate.equals(Compare.eq) || predicate.equals(Compare.lte))));
                            dismissCountIs &= curr.getLabels().isEmpty() && isStep.getLabels().isEmpty()
                                    && isStep.getNextStep() instanceof EmptyStep
                                    && (highRange == 1L && (predicate.equals(Compare.gt) || predicate.equals(Compare.gte)));
                        }
                        if (hasGtOrLtNegative) {
                            // TINKERPOP-2893. Certain combinations of AND and OR will cause this optimization to be incorrect.
                            // E.g. is(lt(x)) where x < 0, is always false since count() will return at least 0.
                            // In this case, ANDing a "less than negative" predicate with another predicate should also return false regardless of highRange.
                            useNotStep = false;
                            dismissCountIs = false;
                        }
                    } else {
                        final Long highRangeOffset = RANGE_PREDICATES.get(predicate);
                        if (value instanceof Collection && highRangeOffset != null) {
                            final Object high = Collections.max((Collection) value);
                            if (high instanceof Number) {
                                final Long highRangeCandidate = ((Number) high).longValue() + highRangeOffset;
                                final boolean update = highRange == null || highRangeCandidate > highRange;
                                if (update) highRange = highRangeCandidate;
                            }
                        }
                    }
                }
                if (highRange != null) {
                    if (useNotStep || dismissCountIs) {
                        traversal.asAdmin().removeStep(isStep); // IsStep
                        traversal.asAdmin().removeStep(curr); // CountStep
                        size -= 2;
                        if (!dismissCountIs) {
                            if (parent instanceof ConnectiveStep) {
                                // wrap the child of and()/or() in not().
                                final Step<?, ?> notStep = transformToNotStep(traversal, parent);
                                TraversalHelper.removeAllSteps(traversal);
                                traversal.addStep(notStep);
                            } else if (parent instanceof FilterStep) {
                                // converts entire where(<x>.count().is(0)) to not(<x>). that's why ConnectiveStep
                                // is handled differently above.
                                final Step filterStep = parent.asStep();
                                final Step<?, ?> notStep = transformToNotStep(traversal, parent);
                                TraversalHelper.replaceStep(filterStep, notStep, filterStep.getTraversal());
                            } else {
                                final Traversal.Admin inner;
                                if (prev != null) {
                                    inner = __.start().asAdmin();
                                    for (; ; ) {
                                        final Step pp = prev.getPreviousStep();
                                        inner.addStep(0, prev);
                                        if (pp instanceof EmptyStep || pp instanceof GraphStep ||
                                                !(prev instanceof FilterStep || prev instanceof SideEffectStep)) break;
                                        traversal.removeStep(prev);
                                        prev = pp;
                                        size--;
                                    }
                                } else {
                                    inner = __.identity().asAdmin();
                                }
                                if (prev != null)
                                    TraversalHelper.replaceStep(prev, new NotStep<>(traversal, inner), traversal);
                                else
                                    traversal.asAdmin().addStep(new NotStep<>(traversal, inner));
                            }
                        } else if (size == 0) {
                            final Step parentStep = traversal.getParent().asStep();
                            if (!(parentStep instanceof EmptyStep)) {
                                final Traversal.Admin parentTraversal = parentStep.getTraversal();
                                TraversalHelper.replaceStep(parentStep, new IdentityStep<>(parentTraversal), parentTraversal);
                            }
                        }
                    } else {
                        final long limit = highRange < 0 ? 0 : highRange;
                        if (isLimitSafe(traversal, isStep, isStepPredicate, limit)) {
                            TraversalHelper.insertBeforeStep(new RangeGlobalStep<>(traversal, 0L, limit), curr, traversal);
                        }
                    }
                    i++;
                }
            }
            prev = curr;
        }
    }

    /**
     * Determines if a {@code limit(n)} can be placed in front of {@code count().is(predicate)} without changing the
     * result. The limit caps the count at {@code n}, which is enough for the {@link IsStep} to decide, but if the
     * predicate accepts {@code n} then the capped value passes through in place of the real count. That is only
     * acceptable when nothing downstream observes the count value (e.g. a trailing {@code is()} in
     * {@code count().is(gt(1)).is(lt(3))}, the root traversal or a {@code map()} child) and the child is only used as
     * a filter.
     */
    private static boolean isLimitSafe(final Traversal.Admin<?, ?> traversal, final IsStepContract isStep,
                                       final P isStepPredicate, final long limit) {
        // if the capped count cannot pass the predicate, any count that does pass is exact
        return !isStepPredicate.test(limit) || !isCountValueObserved(traversal, isStep);
    }

    /**
     * Determines if the value emitted by {@code count().is(predicate)} is visible beyond a simple filter decision.
     */
    private static boolean isCountValueObserved(final Traversal.Admin<?, ?> traversal, final IsStepContract isStep) {
        // a later step can access the count value. this includes the WhereEndStep that where() appends when its
        // child ends with a label, as that compares the value to the path. labels elsewhere in a filter child can't
        // be read from outside of it, so they don't matter here.
        if (!(isStep.getNextStep() instanceof EmptyStep))
            return true;

        // the repeat() body always ends with a RepeatEndStep so it was already handled above, which leaves until()
        // and emit() where the result is only used as a condition
        if (traversal.getParent() instanceof RepeatStep) return false;

        final TraversalParent parent = traversal.getParent();
        // only parents that use the child traversal purely to test for the existence of a result. other FilterStep
        // or SideEffectStep parents (e.g. dedup().by() or aggregate().by()) consume the value itself.
        return !(parent instanceof TraversalFilterStep || parent instanceof NotStep
                || parent instanceof ConnectiveStep || parent instanceof TraversalSideEffectStep);
    }

    /**
     * The {@code traversal} argument was marked as one that could be converted to {@code not()}, so wrap it inside
     * that step and copy the labels.
     */
    private Step<?, ?> transformToNotStep(final Traversal.Admin<?, ?> traversal, final TraversalParent parent) {
        final Step<?, ?> filterStep = parent.asStep();
        final Traversal.Admin<?,?> parentTraversal = filterStep.getTraversal();
        final Step<?,?> notStep = new NotStep<>(parentTraversal,
                traversal.getSteps().isEmpty() ? __.identity() : traversal.clone());
        filterStep.getLabels().forEach(notStep::addLabel);
        return notStep;
    }

    private boolean doStrategy(final Step step) {
        if (!(step instanceof CountGlobalStep) ||
                !(step.getNextStep() instanceof IsStepContract) ||
                step.getPreviousStep() instanceof RangeGlobalStepContract) // if a RangeStep was provided, assume that the user knows what he's doing
            return false;

        final Step parent = step.getTraversal().getParent().asStep();
        return (parent instanceof FilterStep || parent.getLabels().isEmpty()) && // if the parent is labeled, then the count matters
                !(parent.getNextStep() instanceof MatchStep.MatchEndStep && // if this is in a pattern match, then don't do it.
                        ((MatchStep.MatchEndStep) parent.getNextStep()).getMatchKey().isPresent()) &&
                // a traversal in the predicate (e.g. is(lt(__.constant(3))) or is(lte(__.select("a")))) is only
                // resolved at runtime, so no limit can be derived that is guaranteed not to change the result
                !((IsStepContract<?>) step.getNextStep()).getPredicate().hasTraversal();
    }
}
