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
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ClassFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * FilterRankingStrategy reorders filter- and order-steps according to their rank. Labeled steps or lambda holders or
 * steps that contain child traversals, that have labeled steps or lambda holders, will not be reordered.
 * <p/>
 * <table>
 * <thead>
 * <tr><th>Step</th><th>Rank</th></tr>
 * </thead>
 * <tbody>
 * <tr><td>is(predicate)</td><td>1</td></tr>
 * <tr><td>has(predicate)</td><td>2</td></tr>
 * <tr><td>where(predicate)</td><td>3</td></tr>
 * <tr><td>simplePath()</td><td>4</td></tr>
 * <tr><td>cyclicPath()</td><td>4</td></tr>
 * <tr><td>filter(traversal)</td><td>5</td></tr>
 * <tr><td>not(traversal)</td>td>5</td></tr>
 * <tr><td>where(traversal)</td><td>6</td></tr>
 * <tr><td>or(...)</td><td>7</td></tr>
 * <tr><td>and(...)</td><td>8</td></tr>
 * <tr><td>dedup()</td><td>9</td></tr>
 * <tr><td>order()</td><td>10</td></tr>
 * </tbody>
 * </table>
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

    private FilterRankingStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        boolean modified = true;
        while(modified) {
            modified = false;
            for (final Step<?, ?> step : traversal.getSteps()) {
                if (step instanceof FilterStep && !step.getLabels().isEmpty()) {
                    final Step<?, ?> nextStep = step.getNextStep();
                    if (nextStep instanceof FilterStep && !(nextStep instanceof TraversalParent)) {
                        TraversalHelper.copyLabels(step, nextStep, true);
                        modified = true;
                    }
                }
            }
            final List<Step> steps = traversal.getSteps();
            int prevRank = 0;
            for (int i = steps.size() - 1; i >= 0; i--) {
                final Step curr = steps.get(i);
                final int rank = rank(curr);
                if (prevRank > 0 && rank > prevRank) {
                    final Step next = curr.getNextStep();
                    traversal.removeStep(next);
                    traversal.addStep(i, next);
                    modified = true;
                }
                prevRank = rank;
            }
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return PRIORS;
    }

    public static FilterRankingStrategy instance() {
        return INSTANCE;
    }

    /**
     * Ranks the given step. Steps with lower ranks can be moved in front of steps with higher ranks. 0 means that
     * the step has no rank and thus is not exchangeable with its neighbors.
     *
     * @param step the step to get a ranking for
     * @return The rank of the given step.
     */
    private static int getStepRank(final Step step) {
        if (!(step instanceof FilterStep || step instanceof OrderGlobalStep))
            return 0;
        else if (step instanceof IsStep || step instanceof ClassFilterStep)
            return 1;
        else if (step instanceof HasStep)
            return 2;
        else if (step instanceof WherePredicateStep)
            return 3;
        else if (step instanceof SimplePathStep || step instanceof CyclicPathStep)
            return 4;
        else if (step instanceof TraversalFilterStep || step instanceof NotStep)
            return 5;
        else if (step instanceof WhereTraversalStep)
            return 6;
        else if (step instanceof OrStep)
            return 7;
        else if (step instanceof AndStep)
            return 8;
        else if (step instanceof DedupGlobalStep)
            return 9;
        else if (step instanceof OrderGlobalStep)
            return 10;
        else
            return 0;
    }

    /**
     * If the given step has no child traversal that holds a lambda, then the actual rank determined by
     * {@link #getStepRank(Step)} is returned, otherwise 0.
     *
     * @param step the step to get a ranking for
     * @return The rank of the given step.
     */
    private static int rank(final Step step) {
        if (isNotOptimizableStep(step)) {
            return 0;
        }
        final int rank = getStepRank(step);
        if (rank > 0 && step instanceof TraversalParent) {
            final TraversalParent tp = (TraversalParent) step;
            final Iterator<Traversal.Admin<Object, Object>> childTraversalIterator = IteratorUtils.concat(
                    tp.getLocalChildren().iterator(), tp.getGlobalChildren().iterator());
            while (childTraversalIterator.hasNext()) {
                if (TraversalHelper.anyStepRecursively(FilterRankingStrategy::isNotOptimizableStep, childTraversalIterator.next())) {
                    return 0;
                }
            }
        }
        return rank;
    }

    /**
     * Returns true if the step is not optimizable, otherwise false. A step is not optimizable if it (or any of its
     * child traversals) is a lambda holder or has a label.
     *
     * @param step the step to check for optimizability
     * @return true if the given step is optimizable, otherwise false.
     */
    private static boolean isNotOptimizableStep(final Step<?, ?> step) {
        if (step instanceof LambdaHolder)
            return true;
        else {
            for (final String label : step.getLabels()) {
                if (!Graph.Hidden.isHidden(label))
                    return true;
            }
            return false;
        }
    }
}
