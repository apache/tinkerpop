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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MatchWhereStrategy will fold any post-{@code where()} step that maintains a traversal constraint into
 * {@code match()}. {@link MatchStep} is intelligent with traversal constraint applications and thus, can more
 * efficiently use the constraint of {@link WhereTraversalStep} or {@link WherePredicateStep}.
 * <p/>
 * <p/>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @example <pre>
 * __.match(a,b).where(c)            // is replaced by __.match(a,b,c)
 * __.match(a,b).select().where(c)  // is replaced by __.match(a,b,c).select()
 * </pre>
 */
public final class MatchPredicateStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final MatchPredicateStrategy INSTANCE = new MatchPredicateStrategy();
    private static final Set<Class<? extends OptimizationStrategy>> PRIORS = new HashSet<>();

    static {
        PRIORS.add(IdentityRemovalStrategy.class);
    }

    private MatchPredicateStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!TraversalHelper.hasStepOfClass(MatchStep.class, traversal))
            return;

        TraversalHelper.getStepsOfClass(MatchStep.class, traversal).forEach(matchStep -> {
            // match().select().where() --> match(where()).select()
            // match().select().dedup() --> match(dedup()).select()
            Step<?, ?> nextStep = matchStep.getNextStep();
            while (nextStep instanceof WherePredicateStep ||
                    nextStep instanceof WhereTraversalStep ||
                    (nextStep instanceof DedupGlobalStep && !((DedupGlobalStep) nextStep).getScopeKeys().isEmpty() && ((DedupGlobalStep) nextStep).getLocalChildren().isEmpty()) ||
                    (nextStep instanceof SelectStep && ((SelectStep) nextStep).getLocalChildren().isEmpty()) ||
                    (nextStep instanceof SelectOneStep && ((SelectOneStep) nextStep).getLocalChildren().isEmpty())) {
                if (nextStep instanceof WherePredicateStep || nextStep instanceof WhereTraversalStep) {
                    traversal.removeStep(nextStep);
                    matchStep.addGlobalChild(new DefaultTraversal<>().addStep(nextStep));
                    nextStep = matchStep.getNextStep();
                } else if (nextStep instanceof DedupGlobalStep && !((DedupGlobalStep) nextStep).getScopeKeys().isEmpty() && ((DedupGlobalStep) nextStep).getLocalChildren().isEmpty() && !TraversalHelper.onGraphComputer(traversal)) {
                    traversal.removeStep(nextStep);
                    matchStep.setDedupLabels(((DedupGlobalStep<?>) nextStep).getScopeKeys());
                    nextStep = matchStep.getNextStep();
                } else if (nextStep.getLabels().isEmpty()) {
                    nextStep = nextStep.getNextStep();
                } else
                    break;
            }
            // match(as('a').has(key,value),...) --> as('a').has(key,value).match(...)
            final String startLabel = this.determineStartLabelForHasPullOut(matchStep);
            if (null != startLabel) {
                ((MatchStep<?, ?>) matchStep).getGlobalChildren().stream().collect(Collectors.toList()).forEach(matchTraversal -> {
                    if (matchTraversal.getStartStep() instanceof MatchStep.MatchStartStep &&
                            ((MatchStep.MatchStartStep) matchTraversal.getStartStep()).getSelectKey().isPresent() &&
                            ((MatchStep.MatchStartStep) matchTraversal.getStartStep()).getSelectKey().get().equals(startLabel) &&
                            !(matchStep.getPreviousStep() instanceof EmptyStep) &&
                            !matchTraversal.getSteps().stream()
                                    .filter(step -> !(step instanceof MatchStep.MatchStartStep) &&
                                            !(step instanceof MatchStep.MatchEndStep) &&
                                            !(step instanceof HasStep))
                                    .findAny()
                                    .isPresent()) {
                        matchStep.removeGlobalChild(matchTraversal);
                        matchTraversal.removeStep(0);                                       // remove MatchStartStep
                        matchTraversal.removeStep(matchTraversal.getSteps().size() - 1);    // remove MatchEndStep
                        matchStep.getPreviousStep().addLabel(startLabel);
                        TraversalHelper.insertTraversal(matchStep.getPreviousStep(), matchTraversal, traversal);
                    }
                });
            }
        });
    }

    private String determineStartLabelForHasPullOut(final MatchStep<?, ?> matchStep) {
        if (!(matchStep.getTraversal().getParent() instanceof EmptyStep))
            return null;
        else {
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
    }

    public static MatchPredicateStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return PRIORS;
    }
}
