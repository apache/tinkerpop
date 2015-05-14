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
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Bypassing;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.match.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.SupplyingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;

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
            DedupGlobalStep.class, InjectStep.class, MatchStep.class, Mutating.class, SubgraphStep.class
    ));

    private ComputerVerificationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getEngine().isStandard())
            return;

        Step<?, ?> endStep = traversal.getEndStep();
        while (endStep instanceof ComputerAwareStep.EndStep || endStep instanceof ComputerResultStep) {
            endStep = endStep.getPreviousStep();
        }

        if (traversal.getParent() instanceof EmptyStep) {
            if (!(traversal.getStartStep() instanceof GraphStep))
                throw new ComputerVerificationException("GraphComputer does not support traversals starting from a non-GraphStep: " + traversal.getStartStep(), traversal);
            ///
            if (endStep instanceof CollectingBarrierStep && endStep instanceof TraversalParent) {
                if (((TraversalParent) endStep).getLocalChildren().stream().filter(t ->
                        !(t instanceof IdentityTraversal) &&
                                !(t instanceof ConstantTraversal) &&  // for SampleStep
                                !(t instanceof TokenTraversal && ((TokenTraversal) t).getToken().equals(T.id))).findAny().isPresent())
                    throw new ComputerVerificationException("A final CollectingBarrierStep can not process an element beyond its id: " + endStep, traversal);
            }
            ///
            if(endStep instanceof RangeGlobalStep || endStep instanceof TailGlobalStep)
                ((Bypassing) endStep).setBypass(true);
        }

        for (final Step<?, ?> step : traversal.getSteps()) {
            if ((step instanceof ReducingBarrierStep || step instanceof SupplyingBarrierStep || step instanceof OrderGlobalStep || step instanceof RangeGlobalStep || step instanceof TailGlobalStep) && (step != endStep || !(traversal.getParent() instanceof EmptyStep)))
                throw new ComputerVerificationException("Global traversals on GraphComputer may not contain mid-traversal barriers: " + step, traversal);

            if (step instanceof TraversalParent) {
                final Optional<Traversal.Admin<Object, Object>> traversalOptional = ((TraversalParent) step).getLocalChildren().stream()
                        .filter(t -> !TraversalHelper.isLocalStarGraph(t.asAdmin()))
                        .findAny();
                if (traversalOptional.isPresent())
                    throw new ComputerVerificationException("Local traversals on GraphComputer may not traverse past the local star-graph: " + traversalOptional.get(), traversal);
            }

            if (UNSUPPORTED_STEPS.stream().filter(c -> c.isAssignableFrom(step.getClass())).findFirst().isPresent())
                throw new ComputerVerificationException("The following step is currently not supported by GraphComputer traversals: " + step, traversal);

            if (step instanceof PathProcessor && ((PathProcessor) step).getMaxRequirement() != PathProcessor.ElementRequirement.ID)
                throw new ComputerVerificationException("The following path processor step requires more than the element id: " + step + " requires " + ((PathProcessor) step).getMaxRequirement(), traversal);
        }
    }

    public static ComputerVerificationStrategy instance() {
        return INSTANCE;
    }
}
