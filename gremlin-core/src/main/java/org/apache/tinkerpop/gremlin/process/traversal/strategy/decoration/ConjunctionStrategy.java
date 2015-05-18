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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConjunctionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * ConjunctionStrategy rewrites the binary conjunction form of <code>a.and().b</code> into a {@link AndStep} of <code>and(a,b)</code> (likewise for {@link OrStep}.
 * <p/>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @example <pre>
 * __.has("name","stephen").or().has(__.out("knows").has("name","stephen"))   // is replaced by __.or(__.has("name","stephen"),__.has(__.out("knows").has("name","stephen")))
 * __.out("a").out("b").and().out("c").or().out("d")                          // is replaced by __.or(__.and(__.out("a").out("b"), __.out("c")), __.out("d"))
 * </pre>
 */
public final class ConjunctionStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    private static final ConjunctionStrategy INSTANCE = new ConjunctionStrategy();

    private ConjunctionStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!TraversalHelper.hasStepOfAssignableClass(ConjunctionStep.class, traversal))
            return;

        processConjunctionMarker(AndStep.class, traversal);
        processConjunctionMarker(OrStep.class, traversal);
    }

    private static final boolean legalCurrentStep(final Step<?, ?> step) {
        return !(step instanceof EmptyStep || step instanceof OrStep || step instanceof AndStep || step instanceof StartStep);
    }

    private static final void processConjunctionMarker(final Class<? extends ConjunctionStep> markerClass, final Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(markerClass, traversal).forEach(markerStep -> {
            Step<?, ?> currentStep = markerStep.getNextStep();
            final Traversal.Admin<?, ?> rightTraversal = __.start().asAdmin();
            while (legalCurrentStep(currentStep)) {
                final Step<?, ?> nextStep = currentStep.getNextStep();
                rightTraversal.addStep(currentStep);
                traversal.removeStep(currentStep);
                currentStep = nextStep;
            }

            currentStep = markerStep.getPreviousStep();
            final Traversal.Admin<?, ?> leftTraversal = __.start().asAdmin();
            while (legalCurrentStep(currentStep)) {
                final Step<?, ?> previousStep = currentStep.getPreviousStep();
                leftTraversal.addStep(0, currentStep);
                traversal.removeStep(currentStep);
                currentStep = previousStep;
            }
            TraversalHelper.replaceStep(markerStep,
                    markerClass.equals(AndStep.class) ?
                            new WhereStep<Object>(traversal, Scope.global, P.traversal(leftTraversal).and(rightTraversal)) :
                            new WhereStep<Object>(traversal, Scope.global, P.traversal(leftTraversal).or(rightTraversal)),
                    traversal);
        });
    }

    public static ConjunctionStrategy instance() {
        return INSTANCE;
    }
}
