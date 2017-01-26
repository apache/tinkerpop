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
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.AdjacentToIncidentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
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

    private static final Set<Class> MULTI_ITERATION_CLASSES = new HashSet<>(Arrays.asList(
            EdgeVertexStep.class,
            LambdaFlatMapStep.class,
            LambdaMapStep.class
    ));


    private SingleIterationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // only process the first traversal step in an OLAP chain
        TraversalHelper.getFirstStepOfAssignableClass(TraversalVertexProgramStep.class, traversal).ifPresent(step -> {
            final Graph graph = traversal.getGraph().orElse(EmptyGraph.instance()); // best guess at what the graph will be as its dynamically determined
            final Traversal.Admin<?, ?> computerTraversal = step.generateProgram(graph, EmptyMemory.instance()).getTraversal().get().clone();
            // does the traversal as it is message pass?
            boolean doesMessagePass = TraversalHelper.hasStepOfAssignableClassRecursively(Scope.global, MULTI_ITERATION_CLASSES, computerTraversal);
            if (!doesMessagePass) {
                for (final VertexStep vertexStep : TraversalHelper.getStepsOfAssignableClassRecursively(Scope.global, VertexStep.class, computerTraversal)) {
                    if (vertexStep.returnsVertex() || !vertexStep.getDirection().equals(Direction.OUT)) { // in-edges require message pass in OLAP
                        doesMessagePass = true;
                        break;
                    }
                }
            } // if the traversal doesn't message pass, then don't try and localize it as its just wasted computation
            if (doesMessagePass) {
                final boolean beyondStarGraph =
                        TraversalHelper.hasStepOfAssignableClassRecursively(Scope.global, LambdaHolder.class, computerTraversal) ||
                                !TraversalHelper.isLocalStarGraph(computerTraversal);
                if (!beyondStarGraph &&                                                                        // if we move beyond the star graph, then localization is not possible.
                        !(computerTraversal.getStartStep().getNextStep() instanceof EmptyStep) &&              // if its just a g.V()/E(), then don't localize
                        !(computerTraversal.getStartStep().getNextStep() instanceof LocalStep) &&              // removes the potential for the infinite recursive application of the traversal
                        !(computerTraversal.getStartStep().getNextStep() instanceof Barrier) &&                // if the second step is a barrier, no point in trying to localize anything
                        !(TraversalHelper.getStepsOfAssignableClass(TraversalParent.class, computerTraversal). // this is a strict precaution that could be loosed with deeper logic on barriers in global children
                                stream().
                                filter(parent -> !parent.getGlobalChildren().isEmpty()).findAny().isPresent())) {

                    final Traversal.Admin<?, ?> newComputerTraversal = step.computerTraversal.getPure();
                    final Traversal.Admin localTraversal = new DefaultGraphTraversal<>();
                    final Step barrier = (Step) TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, newComputerTraversal).orElse(null);
                    final Step endStep = null == barrier ? newComputerTraversal.getEndStep() : barrier.getPreviousStep();
                    if (!(endStep instanceof VertexStep || endStep instanceof EdgeVertexStep)) {
                        TraversalHelper.removeToTraversal(newComputerTraversal.getStartStep().getNextStep(), null == barrier ? EmptyStep.instance() : barrier, localTraversal);
                        assert !localTraversal.getSteps().isEmpty(); // given the if() constraints, this is impossible
                        if (localTraversal.getSteps().size() > 1) { // if its just a single step, a local wrap will not alter its locus of computation
                            if (null == barrier)
                                TraversalHelper.insertTraversal(0, (Traversal.Admin) __.local(localTraversal), newComputerTraversal);
                            else
                                TraversalHelper.insertTraversal(barrier.getPreviousStep(), (Traversal.Admin) __.local(localTraversal), newComputerTraversal);
                            step.setComputerTraversal(newComputerTraversal);
                        }
                    }
                }
            }
        });
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return PRIORS;
    }

    public static SingleIterationStrategy instance() {
        return INSTANCE;
    }
}
