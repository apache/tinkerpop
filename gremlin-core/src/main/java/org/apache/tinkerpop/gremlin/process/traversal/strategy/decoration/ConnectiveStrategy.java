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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.HasNextStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Set;

/**
 * ConnectiveStrategy rewrites the binary conjunction form of {@code a.and().b} into a {@link AndStep} of
 * {@code and(a,b)} (likewise for {@link OrStep}).
 * <p/>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @example <pre>
 * __.has("name","stephen").or().where(__.out("knows").has("name","stephen"))   // is replaced by __.or(__.has("name","stephen"), __.where(__.out("knows").has("name","stephen")))
 * __.out("a").out("b").and().out("c").or().out("d")                            // is replaced by __.or(__.and(__.out("a").out("b"), __.out("c")), __.out("d"))
 * __.as("a").out().as("b").and().as("c").in().as("d")                          // is replaced by __.and(__.as("a").out().as("b"), __.as("c").in().as("d"))
 * </pre>
 */
public final class ConnectiveStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    private static final ConnectiveStrategy INSTANCE = new ConnectiveStrategy();

    private ConnectiveStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.hasStepOfAssignableClass(ConnectiveStep.class, traversal)) {
            processConnectiveMarker(traversal);
        }
    }

    private static boolean legalCurrentStep(final Step<?, ?> step) {
        return !(step instanceof EmptyStep || step instanceof ProfileSideEffectStep || step instanceof HasNextStep ||
                step instanceof ComputerAwareStep.EndStep || (step instanceof StartStep && !StartStep.isVariableStartStep(step)) ||
                GraphStep.isStartStep(step));
    }

    private static void processConnectiveMarker(final Traversal.Admin<?, ?> traversal) {
        processConjunctionMarker(OrStep.class, traversal);
        processConjunctionMarker(AndStep.class, traversal);
    }

    private static void processConjunctionMarker(final Class<? extends ConnectiveStep> markerClass, final Traversal.Admin<?, ?> traversal) {

        TraversalHelper.getStepsOfClass(markerClass, traversal).stream()
                .filter(conjunctionStep -> conjunctionStep.getLocalChildren().isEmpty())
                .findFirst().ifPresent(connectiveStep -> {

            Step<?, ?> currentStep = connectiveStep.getNextStep();
            final Traversal.Admin<?, ?> rightTraversal = __.start().asAdmin();
            if (!connectiveStep.getLabels().isEmpty()) {
                final StartStep<?> startStep = new StartStep<>(rightTraversal);
                final Set<String> conjunctionLabels = ((Step<?, ?>) connectiveStep).getLabels();
                conjunctionLabels.forEach(startStep::addLabel);
                conjunctionLabels.forEach(label -> connectiveStep.removeLabel(label));
                rightTraversal.addStep(startStep);
            }
            while (legalCurrentStep(currentStep)) {
                final Step<?, ?> nextStep = currentStep.getNextStep();
                rightTraversal.addStep(currentStep);
                traversal.removeStep(currentStep);
                currentStep = nextStep;
            }
            processConnectiveMarker(rightTraversal);

            currentStep = connectiveStep.getPreviousStep();
            final Traversal.Admin<?, ?> leftTraversal = __.start().asAdmin();
            while (legalCurrentStep(currentStep)) {
                final Step<?, ?> previousStep = currentStep.getPreviousStep();
                leftTraversal.addStep(0, currentStep);
                traversal.removeStep(currentStep);
                currentStep = previousStep;
            }
            processConnectiveMarker(leftTraversal);

            if (connectiveStep instanceof AndStep)
                TraversalHelper.replaceStep((Step) connectiveStep, new AndStep(traversal, leftTraversal, rightTraversal), traversal);
            else
                TraversalHelper.replaceStep((Step) connectiveStep, new OrStep(traversal, leftTraversal, rightTraversal), traversal);
        });
    }

    public static ConnectiveStrategy instance() {
        return INSTANCE;
    }
}