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
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ClassFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.javatuples.Pair;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * {@code FilterRankingStrategy} reorders filter- and order-steps according to their rank. Step ranks are defined within
 * the strategy and indicate when it is reasonable for a step to move in front of another. It will also do its best to
 * push step labels as far "right" as possible in order to keep traversers as small and bulkable as possible prior to
 * the absolute need for path-labeling.
 *
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @example <pre>
 * __.order().dedup()                        // is replaced by __.dedup().order()
 * __.dedup().filter(out()).has("value", 0)  // is replaced by __.has("value", 0).filter(out()).dedup()
 * </pre>
 */
public final class FilterRankingStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final FilterRankingStrategy INSTANCE = new FilterRankingStrategy();
    private static final Set<Class<? extends OptimizationStrategy>> PRIORS = Collections.singleton(IdentityRemovalStrategy.class);

    private FilterRankingStrategy() { }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // this strategy is only applied to the root (recursively) because it uses a tiny cache which will drastically
        // speed up the ranking function in the event of encountering traversals with significant depth and branching.
        // if we let normal strategy application apply then the cache will reset since this strategy and the traversal
        // does not hold any state about that cache. tried using the marker pattern used in other strategies but that
        // didn't work so well.
        if (traversal.isRoot()) {
            // TraversalParent steps require a costly function to calculate if labels are in use in their child
            // traversals. This little cache keeps the effective data of that function which is if there is a
            // lambda in the children and the set of scope keys. note that the lambda sorta trumps the labels in
            // that if there is a lambda there's no real point to doing any sort of eval of the labels.
            final Map<TraversalParent, Pair<Boolean, Set<String>>> traversalParentCache = new HashMap<>();
            final Map<Step, Integer> stepRanking = new HashMap<>();

            // build up the little cache
            final Map<TraversalParent, List<Step<?,?>>> m =
                    TraversalHelper.getStepsOfAssignableClassRecursively(traversal, Scoping.class, LambdaHolder.class).stream().
                    collect(Collectors.groupingBy(step -> ((Step) step).getTraversal().getParent()));
            m.forEach((k, v) -> {
                final boolean hasLambda = v.stream().anyMatch(s -> s instanceof LambdaHolder);
                if (hasLambda) {
                    traversalParentCache.put(k, Pair.with(true, Collections.emptySet()));
                } else {
                    traversalParentCache.put(k, Pair.with(false, v.stream().filter(s -> s instanceof Scoping).
                            flatMap(s -> ((Scoping) s).getScopeKeys().stream()).collect(Collectors.toSet())));
                }
            });

            TraversalHelper.applyTraversalRecursively(t -> {
                boolean modified = true;
                while (modified) {
                    modified = false;
                    final List<Step> steps = t.getSteps();
                    for (int i = 0; i < steps.size() - 1; i++) {
                        final Step<?, ?> step = steps.get(i);
                        final Set<String> labels = step.getLabels();
                        final Step<?, ?> nextStep = step.getNextStep();
                        if (!usesLabels(nextStep, labels, traversalParentCache)) {
                            final int nextRank = stepRanking.computeIfAbsent(nextStep, FilterRankingStrategy::getStepRank);
                            if (nextRank != 0) {
                                if (!step.getLabels().isEmpty()) {
                                    TraversalHelper.copyLabels(step, nextStep, true);
                                    modified = true;
                                }
                                if (stepRanking.computeIfAbsent(step, FilterRankingStrategy::getStepRank) > nextRank) {
                                    t.removeStep(nextStep);
                                    t.addStep(i, nextStep);
                                    modified = true;
                                }
                            }
                        }
                    }
                }
            }, traversal);
        }
    }

    /**
     * Ranks the given step. Steps with lower ranks can be moved in front of steps with higher ranks. 0 means that
     * the step has no rank and thus is not exchangeable with its neighbors.
     *
     * @param step the step to get a ranking for
     * @return The rank of the given step.
     */
    private static int getStepRank(final Step step) {
        final int rank;
        if (!(step instanceof FilterStep || step instanceof OrderGlobalStep))
            return 0;
        else if (step instanceof IsStep || step instanceof ClassFilterStep)
            rank = 1;
        else if (step instanceof HasStep)
            rank = 2;
        else if (step instanceof WherePredicateStep && ((WherePredicateStep) step).getLocalChildren().isEmpty())
            rank = 3;
        else if (step instanceof TraversalFilterStep || step instanceof NotStep)
            rank = 4;
        else if (step instanceof WhereTraversalStep)
            rank = 5;
        else if (step instanceof OrStep)
            rank = 6;
        else if (step instanceof AndStep)
            rank = 7;
        else if (step instanceof WherePredicateStep) // has by()-modulation
            rank = 8;
        else if (step instanceof DedupGlobalStep)
            rank = 9;
        else if (step instanceof OrderGlobalStep)
            rank = 10;
        else
            return 0;
        ////////////
        if (step instanceof TraversalParent)
            return getMaxStepRank((TraversalParent) step, rank);
        else
            return rank;
    }

    private static int getMaxStepRank(final TraversalParent parent, final int startRank) {
        int maxStepRank = startRank;
        // no filter steps are global parents (yet)
        for (final Traversal.Admin<?, ?> traversal : parent.getLocalChildren()) {
            for (final Step<?, ?> step : traversal.getSteps()) {
                final int stepRank = getStepRank(step);
                if (stepRank > maxStepRank)
                    maxStepRank = stepRank;
            }
        }
        return maxStepRank;
    }

    private static boolean usesLabels(final Step<?, ?> step, final Set<String> labels,
                                      final Map<TraversalParent, Pair<Boolean, Set<String>>> traversalParentCache) {
        if (step instanceof LambdaHolder)
            return true;
        if (step instanceof Scoping && !labels.isEmpty()) {
            final Set<String> scopes = ((Scoping) step).getScopeKeys();
            for (final String label : labels) {
                if (scopes.contains(label))
                    return true;
            }
        }

        if (step instanceof TraversalParent) {
            // when the step is a parent and is not in the cache it means it's not gonna be using labels
            if (!traversalParentCache.containsKey(step)) return false;

            // if we do have a pair then check the boolean first, as it instantly means labels are in use
            // (or i guess can't be detected because it's a lambda)
            final Pair<Boolean, Set<String>> p = traversalParentCache.get(step);
            if (p.getValue0())
                return true;
            else
                return p.getValue1().stream().anyMatch(labels::contains);
        }
        return false;
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return PRIORS;
    }

    public static FilterRankingStrategy instance() {
        return INSTANCE;
    }
}