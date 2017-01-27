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

package org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.util.EmptyMemory;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.AdjacentToIncidentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SingleIterationStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final SingleIterationStrategy INSTANCE = new SingleIterationStrategy();

    private static final Set<Class<? extends OptimizationStrategy>> PRIORS = new HashSet<>(Arrays.asList(
            IncidentToAdjacentStrategy.class,
            AdjacentToIncidentStrategy.class,
            FilterRankingStrategy.class,
            InlineFilterStrategy.class));

    private SingleIterationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // only process the first traversal step in an OLAP chain
        TraversalHelper.getFirstStepOfAssignableClass(TraversalVertexProgramStep.class, traversal).ifPresent(step -> {
            final Graph graph = traversal.getGraph().orElse(EmptyGraph.instance()); // best guess at what the graph will be as its dynamically determined
            final Traversal.Admin<?, ?> compiledComputerTraversal = step.generateProgram(graph, EmptyMemory.instance()).getTraversal().get().clone();
            if (!compiledComputerTraversal.isLocked())
                compiledComputerTraversal.applyStrategies();
            if (!TraversalHelper.hasStepOfAssignableClassRecursively(Arrays.asList(LocalStep.class, LambdaHolder.class), compiledComputerTraversal) &&
                    !compiledComputerTraversal.getTraverserRequirements().contains(TraverserRequirement.PATH) &&            // when dynamic detachment is provided in 3.3.0, remove this (TODO)
                    !compiledComputerTraversal.getTraverserRequirements().contains(TraverserRequirement.LABELED_PATH) &&    // when dynamic detachment is provided in 3.3.0, remove this (TODO)
                    !(TraversalHelper.getStepsOfAssignableClass(TraversalParent.class, compiledComputerTraversal).          // this is a strict precaution that could be loosed with deeper logic on barriers in global children
                            stream().
                            filter(parent -> !parent.getGlobalChildren().isEmpty()).findAny().isPresent())) {
                final Traversal.Admin<?, ?> computerTraversal = step.computerTraversal.getPure();
                if (computerTraversal.getSteps().size() > 1 &&
                        !(computerTraversal.getStartStep().getNextStep() instanceof Barrier) &&
                        TraversalHelper.hasStepOfAssignableClassRecursively(Arrays.asList(VertexStep.class, EdgeVertexStep.class), computerTraversal) &&
                        TraversalHelper.isLocalStarGraph(computerTraversal)) {
                    final Traversal.Admin newChildTraversal = new DefaultGraphTraversal<>();
                    final Step barrier = (Step) TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, computerTraversal).orElse(null);
                    if (SingleIterationStrategy.insertElementId(barrier)) // out().count() -> out().id().count()
                        TraversalHelper.insertBeforeStep(new IdStep(computerTraversal), barrier, computerTraversal);
                    if (!(SingleIterationStrategy.endsWithElement(null == barrier ? computerTraversal.getEndStep() : barrier.getPreviousStep()))) {
                        TraversalHelper.removeToTraversal(computerTraversal.getStartStep() instanceof GraphStep ?
                                computerTraversal.getStartStep().getNextStep() :
                                (Step) computerTraversal.getStartStep(), null == barrier ?
                                EmptyStep.instance() :
                                barrier, newChildTraversal);
                        if (null == barrier)
                            TraversalHelper.insertTraversal(0, (Traversal.Admin) __.local(newChildTraversal), computerTraversal);
                        else
                            TraversalHelper.insertTraversal(barrier.getPreviousStep(), (Traversal.Admin) __.local(newChildTraversal), computerTraversal);
                    }
                }
                step.setComputerTraversal(computerTraversal);
            }
        });
    }

    private static boolean insertElementId(final Step<?, ?> barrier) {
        if (!(barrier instanceof Barrier))
            return false;
        else if (!endsWithElement(barrier.getPreviousStep()))
            return false;
        else if (barrier instanceof CountGlobalStep)
            return true;
        else if (barrier instanceof DedupGlobalStep &&
                ((DedupGlobalStep) barrier).getScopeKeys().isEmpty() &&
                ((DedupGlobalStep) barrier).getLocalChildren().isEmpty() &&
                barrier.getNextStep() instanceof CountGlobalStep)
            return true;
        else
            return false;
    }

    private static boolean endsWithElement(Step<?, ?> currentStep) {
        while (!(currentStep instanceof EmptyStep)) {
            if (currentStep instanceof VertexStep || currentStep instanceof EdgeVertexStep) // TODO: add GraphStep but only if its mid-traversal V()/E()
                return true;
            else if (currentStep instanceof TraversalFlatMapStep || currentStep instanceof TraversalMapStep)
                return endsWithElement(((TraversalParent) currentStep).getLocalChildren().get(0).getEndStep());
            else if (!(currentStep instanceof FilterStep || currentStep instanceof SideEffectStep))
                return false;
            currentStep = currentStep.getPreviousStep();
        }
        return false;
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return PRIORS;
    }

    public static SingleIterationStrategy instance() {
        return INSTANCE;
    }
}
