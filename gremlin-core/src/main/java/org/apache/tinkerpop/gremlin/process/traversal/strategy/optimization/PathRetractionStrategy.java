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
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.PathUtil;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Ted Wilmes (http://twilmes.org)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PathRetractionStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    public static Integer DEFAULT_STANDARD_BARRIER_SIZE = 2500;

    private static final PathRetractionStrategy INSTANCE = new PathRetractionStrategy(DEFAULT_STANDARD_BARRIER_SIZE);
    // these strategies do strong rewrites involving path labeling and thus, should run prior to PathRetractionStrategy
    private static final Set<Class<? extends OptimizationStrategy>> PRIORS = new HashSet<>(Arrays.asList(
            RepeatUnrollStrategy.class, MatchPredicateStrategy.class, PathProcessorStrategy.class));

    private final int standardBarrierSize;

    private PathRetractionStrategy(final int standardBarrierSize) {
        this.standardBarrierSize = standardBarrierSize;
    }

    public static PathRetractionStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // do not apply this strategy if there are lambdas as you can't introspect to know what path information the lambdas are using
        // do not apply this strategy if a PATH requirement step is being used (in the future, we can do PATH requirement lookhead to be more intelligent about its usage)
        if (TraversalHelper.anyStepRecursively(step -> step instanceof LambdaHolder || step.getRequirements().contains(TraverserRequirement.PATH), TraversalHelper.getRootTraversal(traversal)))
            return;

        final boolean onGraphComputer = TraversalHelper.onGraphComputer(traversal);
        final Set<String> foundLabels = new HashSet<>();
        final Set<String> keepLabels = new HashSet<>();

        final List<Step> steps = traversal.getSteps();
        for (int i = steps.size() - 1; i >= 0; i--) {
            final Step currentStep = steps.get(i);
            // maintain our list of labels to keep, repeatedly adding labels that were found during
            // the last iteration
            keepLabels.addAll(foundLabels);

            final Set<String> labels = PathUtil.getReferencedLabels(currentStep);
            for (final String label : labels) {
                if (foundLabels.contains(label))
                    keepLabels.add(label);
                else
                    foundLabels.add(label);
            }
            // add the keep labels to the path processor
            if (currentStep instanceof PathProcessor) {
                final PathProcessor pathProcessor = (PathProcessor) currentStep;
                if (currentStep instanceof MatchStep && (currentStep.getNextStep().equals(EmptyStep.instance()) || currentStep.getNextStep() instanceof DedupGlobalStep)) {
                    pathProcessor.setKeepLabels(((MatchStep) currentStep).getMatchStartLabels());
                    pathProcessor.getKeepLabels().addAll(((MatchStep) currentStep).getMatchEndLabels());
                } else
                    pathProcessor.setKeepLabels(new HashSet<>(keepLabels));

                if (currentStep.getTraversal().getParent() instanceof MatchStep) {
                    pathProcessor.setKeepLabels(((MatchStep) currentStep.getTraversal().getParent().asStep()).getMatchStartLabels());
                    pathProcessor.getKeepLabels().addAll(((MatchStep) currentStep.getTraversal().getParent().asStep()).getMatchEndLabels());
                }

                // OLTP barrier optimization that will try and bulk traversers after a path processor step to thin the stream
                if (!onGraphComputer &&
                        !(currentStep instanceof MatchStep) &&
                        !(currentStep instanceof Barrier) &&
                        !(currentStep.getNextStep() instanceof Barrier) &&
                        !(currentStep.getTraversal().getParent() instanceof MatchStep))
                    TraversalHelper.insertAfterStep(new NoOpBarrierStep<>(traversal, this.standardBarrierSize), currentStep, traversal);
            }
        }

        keepLabels.addAll(foundLabels);

        // build a list of parent traversals and their required labels
        Step<?, ?> parent = traversal.getParent().asStep();
        final List<Pair<Step, Set<String>>> parentKeeperPairs = new ArrayList<>();
        while (!parent.equals(EmptyStep.instance())) {
            Set<String> parentKeepLabels = new HashSet<>(PathUtil.getReferencedLabels(parent));
            parentKeepLabels.addAll(PathUtil.getReferencedLabelsAfterStep(parent));
            parentKeeperPairs.add(new Pair<>(parent, parentKeepLabels));
            parent = parent.getTraversal().getParent().asStep();
        }

        // reverse the parent traversal list so that labels are kept from the top down
        Collections.reverse(parentKeeperPairs);

        boolean hasRepeat = false;

        final Set<String> keeperTrail = new HashSet<>();
        for (final Pair<Step, Set<String>> pair : parentKeeperPairs) {
            Step step = pair.getValue0();
            final Set<String> levelLabels = pair.getValue1();

            if (step instanceof RepeatStep) {
                hasRepeat = true;
            }

            // propagate requirements of keep labels back through the traversal's previous steps
            // to ensure that the label is not dropped before it reaches the step(s) that require it
            step = step.getPreviousStep();
            while (!(step.equals(EmptyStep.instance()))) {
                if (step instanceof PathProcessor) {
                    if (((PathProcessor) step).getKeepLabels() == null) {
                        ((PathProcessor) step).setKeepLabels(new HashSet<>(keepLabels));
                    } else {
                        ((PathProcessor) step).getKeepLabels().addAll(new HashSet<>(keepLabels));
                    }
                }
                step = step.getPreviousStep();
            }

            // propagate keep labels forwards if future steps require a particular nested label
            while (!(step.equals(EmptyStep.instance()))) {
                if (step instanceof PathProcessor) {
                    final Set<String> referencedLabels = PathUtil.getReferencedLabelsAfterStep(step);
                    for (final String ref : referencedLabels) {
                        if (levelLabels.contains(ref)) {
                            if (((PathProcessor) step).getKeepLabels() == null) {
                                final HashSet<String> newKeepLabels = new HashSet<>();
                                newKeepLabels.add(ref);
                                ((PathProcessor) step).setKeepLabels(newKeepLabels);
                            } else {
                                ((PathProcessor) step).getKeepLabels().addAll(Collections.singleton(ref));
                            }
                        }
                    }
                }

                step = step.getNextStep();
            }

            keeperTrail.addAll(levelLabels);
        }

        for (final Step currentStep : traversal.getSteps()) {
            // go back through current level and add all keepers
            // if there is one more RepeatSteps in this traversal's lineage, preserve keep labels
            if (currentStep instanceof PathProcessor) {
                ((PathProcessor) currentStep).getKeepLabels().addAll(keeperTrail);
                if (hasRepeat) {
                    ((PathProcessor) currentStep).getKeepLabels().addAll(keepLabels);
                }
            }
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return PRIORS;
    }
}
