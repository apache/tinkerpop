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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ComputerVerificationStrategy extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy> implements TraversalStrategy.VerificationStrategy {

    private static final ComputerVerificationStrategy INSTANCE = new ComputerVerificationStrategy();
    private static final Set<Class<?>> UNSUPPORTED_STEPS = new HashSet<>(Arrays.asList(
            InjectStep.class, Mutating.class, SubgraphStep.class, ComputerResultStep.class
    ));

    private ComputerVerificationStrategy() {
    }

    private static void onlyGlobalChildren(final Traversal.Admin<?, ?> traversal) {
        for (final Step step : traversal.getSteps()) {
            if (step instanceof GraphComputing)
                ((GraphComputing) step).onGraphComputer();
            if (step instanceof TraversalParent) {
                ((TraversalParent) step).getGlobalChildren().forEach(ComputerVerificationStrategy::onlyGlobalChildren);
            }
        }
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {

        if (!TraversalHelper.onGraphComputer(traversal) || traversal.getParent().isLocalChild(traversal))  // only process global children as local children are standard semantics
            return;

        Step<?, ?> endStep = traversal.getEndStep();
        while (endStep instanceof ComputerAwareStep.EndStep) {
            endStep = endStep.getPreviousStep();
        }

        if (traversal.getParent() instanceof TraversalVertexProgramStep) {
            if (traversal.getStartStep() instanceof GraphStep && traversal.getSteps().stream().filter(step -> step instanceof GraphStep).count() > 1)
                throw new VerificationException("GraphComputer does not support mid-traversal V()/E()", traversal);

            ComputerVerificationStrategy.onlyGlobalChildren(traversal);
        }

        for (final Step<?, ?> step : traversal.getSteps()) {

            // you can not traverse past the local star graph with localChildren (e.g. by()-modulators).
            if (step instanceof TraversalParent) {
                final Optional<Traversal.Admin<Object, Object>> traversalOptional = ((TraversalParent) step).getLocalChildren().stream()
                        .filter(t -> !TraversalHelper.isLocalStarGraph(t.asAdmin()))
                        .findAny();
                if (traversalOptional.isPresent())
                    throw new VerificationException("Local traversals on GraphComputer may not traverse past the local star-graph: " + traversalOptional.get(), traversal);
            }

            // collecting barriers and dedup global use can only operate on the element and its properties (no incidences)
            if ((step instanceof CollectingBarrierStep || step instanceof DedupGlobalStep) && step instanceof TraversalParent) {
                if (((TraversalParent) step).getLocalChildren().stream().filter(t -> !TraversalHelper.isLocalProperties(t)).findAny().isPresent())
                    throw new VerificationException("A barrier-steps can not process the incident edges of a vertex: " + step, traversal);
            }

            // this is due to how reducing works and can be fixed by generalizing the current simple model
            // we need to make it so dedup global does its project post reduction (easy to do, just do it)
            if ((step instanceof DedupGlobalStep) && (!((DedupGlobalStep) step).getScopeKeys().isEmpty())) {
                throw new VerificationException("A dedup()-step can not process scoped elements: " + step, traversal);
            }

            // this is a problem because sideEffect.merge() is transient on the OLAP reduction
            if (TraversalHelper.getRootTraversal(traversal).getTraverserRequirements().contains(TraverserRequirement.ONE_BULK))
                throw new VerificationException("One bulk us currently not supported: " + step, traversal);

            if ((step instanceof WhereTraversalStep && TraversalHelper.getVariableLocations(((WhereTraversalStep<?>) step).getLocalChildren().get(0)).contains(Scoping.Variable.START)))
                throw new VerificationException("A where()-step that has a start variable is not allowed because the variable value is retrieved from the path: " + step, traversal);

            if (step instanceof PathProcessor && ((PathProcessor) step).getMaxRequirement() != PathProcessor.ElementRequirement.ID)
                throw new VerificationException("The following path processor step requires more than the element id: " + step + " requires " + ((PathProcessor) step).getMaxRequirement(), traversal);

            if (UNSUPPORTED_STEPS.stream().filter(c -> c.isAssignableFrom(step.getClass())).findFirst().isPresent())
                throw new VerificationException("The following step is currently not supported by GraphComputer traversals: " + step, traversal);

        }

        Step<?, ?> nextParentStep = traversal.getParent().asStep();
        while (!(nextParentStep instanceof EmptyStep)) {
            if (nextParentStep instanceof PathProcessor && ((PathProcessor) nextParentStep).getMaxRequirement() != PathProcessor.ElementRequirement.ID)
                throw new VerificationException("The following path processor step requires more than the element id: " + nextParentStep + " requires " + ((PathProcessor) nextParentStep).getMaxRequirement(), traversal);
            nextParentStep = nextParentStep.getNextStep();
        }
    }

    public static ComputerVerificationStrategy instance() {
        return INSTANCE;
    }
}
