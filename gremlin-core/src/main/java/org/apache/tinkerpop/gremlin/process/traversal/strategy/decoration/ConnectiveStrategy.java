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

import java.util.Collections;
import java.util.List;
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

        final List<Step> steps = traversal.getSteps();
        for (int i = 0; i < steps.size(); i++) {
            final Step step = steps.get(i);
            if (step.getClass().equals(markerClass)) {
                final ConnectiveStep<?> currentStep = (ConnectiveStep) step;
                if (currentStep.getLocalChildren().isEmpty()) {
                    Traversal.Admin<?, ?> connectiveTraversal;
                    currentStep.addLocalChild(connectiveTraversal = __.start().asAdmin());
                    for (int j = i - 1; j >= 0; i--, j--) {
                        final Step previousStep = steps.get(j);
                        if (legalCurrentStep(previousStep)) {
                            connectiveTraversal.addStep(0, previousStep);
                            traversal.removeStep(previousStep);
                        } else break;
                    }
                    i++;
                    currentStep.addLocalChild(connectiveTraversal = connectiveTraversal(connectiveTraversal, currentStep));
                    currentStep.getLabels().forEach(currentStep::removeLabel);
                    while (i < steps.size()) {
                        final Step nextStep = steps.get(i);
                        if (legalCurrentStep(nextStep)) {
                            if (nextStep.getClass().equals(markerClass) &&
                                    ((ConnectiveStep) nextStep).getLocalChildren().isEmpty()) {
                                final ConnectiveStep<?> nextConnectiveStep = (ConnectiveStep<?>) nextStep;
                                currentStep.addLocalChild(connectiveTraversal = connectiveTraversal(connectiveTraversal, nextConnectiveStep));
                            } else {
                                connectiveTraversal.addStep(nextStep);
                            }
                            traversal.removeStep(nextStep);
                        } else break;
                    }
                    if (currentStep instanceof OrStep) {
                        currentStep.getLocalChildren().forEach(t -> processConjunctionMarker(AndStep.class, t));
                    }
                }
            }
        }
    }

    private static Traversal.Admin<?,?> connectiveTraversal(final Traversal.Admin<?, ?> connectiveTraversal,
                                                            final ConnectiveStep connectiveStep) {
        final Traversal.Admin<?, ?> traversal = __.start().asAdmin();
        final Set<String> conjunctionLabels = connectiveStep.getLabels();
        if (!conjunctionLabels.isEmpty()) {
            final StartStep<?> startStep = new StartStep<>(connectiveTraversal);
            conjunctionLabels.forEach(startStep::addLabel);
            traversal.addStep(startStep);
        }
        return traversal;
    }

    public static ConnectiveStrategy instance() {
        return INSTANCE;
    }
}