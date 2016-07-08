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
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.PathUtil;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Ted Wilmes (http://twilmes.org)
 */
public final class PrunePathStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final PrunePathStrategy INSTANCE = new PrunePathStrategy();
    private static final Set<Class<? extends OptimizationStrategy>> PRIORS = new HashSet<>();

    static {
        PRIORS.add(PathProcessorStrategy.class);
    }

    private PrunePathStrategy() {
    }

    public static PrunePathStrategy instance() {
        return INSTANCE;
    }

    protected Set<String> getAndPropagateReferencedLabels(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getParent().equals(EmptyStep.instance())) {
            return Collections.EMPTY_SET;
        }
        Step<?, ?> parent = traversal.getParent().asStep();
        Set<String> referencedLabels = new HashSet<>();
        // get referenced labels from this traversal
        referencedLabels.addAll(PathUtil.getReferencedLabels(traversal));
        Set<String> topLevelLabels = new HashSet<>();
        while (true) {
            // is this parent step in the top level traversal? If so, walk forwards and gather labels
            // that should be kept because they are required in latter parts of the traversal
            Step<?, ?> step;
            boolean topLevelParent = false;
            if (parent.getTraversal().getParent().equals(EmptyStep.instance())) {
                step = parent;
                topLevelParent = true;
            } else {
                // start at the beginning of the traversal
                step = parent.getTraversal().getStartStep();
            }
            do {
                Set<String> labels = PathUtil.getReferencedLabels(step);
                if (topLevelParent) {
                    topLevelLabels.addAll(labels);
                } else {
                    referencedLabels.addAll(labels);
                }
                step = step.getNextStep();
            } while(!(step.equals(EmptyStep.instance())));
            if (topLevelParent) {
                step = parent;
                do {
                    // if this is the top level traversal, propagate all nested labels
                    // to previous PathProcess steps
                    if (step instanceof PathProcessor) {
                        ((PathProcessor) step).getKeepLabels().addAll(referencedLabels);
                    }
                    step = step.getPreviousStep();
                } while (!(step.equals(EmptyStep.instance())));
                break;
            } else {
                parent = parent.getTraversal().getParent().asStep();
            }
        }
        referencedLabels.addAll(topLevelLabels);
        return referencedLabels;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalParent parent = traversal.getParent();

        Set<String> foundLabels = new HashSet<>();
        Set<String> keepLabels = new HashSet<>();

        // If this traversal has a parent, it will need to inherit its
        // parent's keep labels.  If its direct parent is not a PathProcessor,
        // walk back up to the top level traversal and work forwards to determine which labels
        // must be kept.
        if (!parent.equals(EmptyStep.instance())) {
            // start with parents keep labels
            if (parent instanceof PathProcessor) {
                PathProcessor parentPathProcess = (PathProcessor) parent;
                if (parentPathProcess.getKeepLabels() != null) keepLabels.addAll(parentPathProcess.getKeepLabels());
            } else {
                Set<String> labels = getAndPropagateReferencedLabels(traversal);
                keepLabels.addAll(labels);
            }
        }

        // check if the traversal contains any path or subgraph steps and if
        // it does, note it so that the keep labels are set to null later on
        // which signals PathProcessors to not drop path information
        boolean hasPathStep = false;
        final List<PathStep> pathSteps = TraversalHelper.getStepsOfAssignableClassRecursively(PathStep.class, traversal);
        final List<SubgraphStep> subgraphSteps = TraversalHelper.getStepsOfAssignableClassRecursively(SubgraphStep.class, traversal);
        if (!pathSteps.isEmpty() || !subgraphSteps.isEmpty()) {
            hasPathStep = true;
        }

        final List<Step> steps = traversal.getSteps();
        for(int i = steps.size() - 1; i >= 0; i--) {
            Step currentStep = steps.get(i);
            if (!hasPathStep) {
                // maintain our list of labels to keep, repeatedly adding labels that were found during
                // the last iteration
                keepLabels.addAll(foundLabels);

                final Set<String> labels = PathUtil.getReferencedLabels(currentStep);
                for (final String label : labels) {
                    if (foundLabels.contains(label)) {
                        keepLabels.add(label);
                    } else {
                        foundLabels.add(label);
                    }
                }

                if (currentStep instanceof PathProcessor) {
                    ((PathProcessor) currentStep).setKeepLabels(new HashSet<>(keepLabels));
                }
            } else {
                // if there is a PathStep or SubgraphStep in the traversal, do not drop labels
                if (currentStep instanceof PathProcessor) {
                    // set keep labels to null so that no labels are dropped
                    ((PathProcessor) currentStep).setKeepLabels(null);
                }
            }
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return PRIORS;
    }
}
