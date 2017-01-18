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
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphStep;
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

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {

        if (!TraversalHelper.onGraphComputer(traversal))
            return;

        final boolean globalChild = TraversalHelper.isGlobalChild(traversal);

        if (traversal.getParent() instanceof TraversalVertexProgramStep) {
            if (TraversalHelper.getStepsOfAssignableClassRecursively(GraphStep.class, traversal).size() > 1)
                throw new VerificationException("Mid-traversal V()/E() is currently not supported on GraphComputer", traversal);
            if (TraversalHelper.hasStepOfAssignableClassRecursively(ProfileStep.class, traversal) && TraversalHelper.getStepsOfAssignableClass(VertexProgramStep.class, TraversalHelper.getRootTraversal(traversal)).size() > 1)
                throw new VerificationException("Profiling a multi-VertexProgramStep traversal is currently not supported on GraphComputer", traversal);
        }

        for (final Step<?, ?> step : traversal.getSteps()) {

            // only global children are graph computing
            if (globalChild && step instanceof GraphComputing)
                ((GraphComputing) step).onGraphComputer();

            // you can not traverse past the local star graph with localChildren (e.g. by()-modulators).
            if (step instanceof TraversalParent) {
                final Optional<Traversal.Admin<Object, Object>> traversalOptional = ((TraversalParent) step).getLocalChildren().stream()
                        .filter(t -> !TraversalHelper.isLocalStarGraph(t.asAdmin()))
                        .findAny();
                if (traversalOptional.isPresent())
                    throw new VerificationException("Local traversals may not traverse past the local star-graph on GraphComputer: " + traversalOptional.get(), traversal);
            }

            // this is a problem because sideEffect.merge() is transient on the OLAP reduction
            if (TraversalHelper.getRootTraversal(traversal).getTraverserRequirements().contains(TraverserRequirement.ONE_BULK))
                throw new VerificationException("One bulk is currently not supported on GraphComputer: " + step, traversal);

            if (step instanceof PathProcessor && ((PathProcessor) step).getMaxRequirement() != PathProcessor.ElementRequirement.ID)
                throw new VerificationException("It is not possible to access more than a path element's id on GraphComputer: " + step + " requires " + ((PathProcessor) step).getMaxRequirement(), traversal);

            if (UNSUPPORTED_STEPS.stream().filter(c -> c.isAssignableFrom(step.getClass())).findFirst().isPresent())
                throw new VerificationException("The following step is currently not supported on GraphComputer: " + step, traversal);

        }

        Step<?, ?> nextParentStep = traversal.getParent().asStep();
        while (!(nextParentStep instanceof EmptyStep)) {
            if (nextParentStep instanceof PathProcessor && ((PathProcessor) nextParentStep).getMaxRequirement() != PathProcessor.ElementRequirement.ID)
                throw new VerificationException("The following path processor step requires more than the element id on GraphComputer: " + nextParentStep + " requires " + ((PathProcessor) nextParentStep).getMaxRequirement(), traversal);
            nextParentStep = nextParentStep.getNextStep();
        }
    }

    public static ComputerVerificationStrategy instance() {
        return INSTANCE;
    }
}
