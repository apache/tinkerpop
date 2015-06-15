/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConjunctionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.exp.XMatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MatchStartEndStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    private static final MatchStartEndStrategy INSTANCE = new MatchStartEndStrategy();

    private static final Set<Class<? extends DecorationStrategy>> PRIORS = new HashSet<>();

    static {
        PRIORS.add(ConjunctionStrategy.class);
    }


    private MatchStartEndStrategy() {
    }

    private static void addSelectOneStartStep(final XMatchStep<?> matchStep, final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> conjunction : TraversalHelper.getStepsOfAssignableClass(ConjunctionStep.class, traversal)) {
            final XMatchStep xMatchStep = new XMatchStep(traversal, conjunction instanceof AndStep ? XMatchStep.Conjunction.AND : XMatchStep.Conjunction.OR, ((ConjunctionStep<?>) conjunction).getLocalChildren().toArray(new Traversal[((ConjunctionStep<?>) conjunction).getLocalChildren().size()]));
            TraversalHelper.replaceStep(conjunction, xMatchStep, traversal);
            addSelectOneStartStep(xMatchStep, traversal);
        }

        // START STEP to SelectOneStep
        final Step<?, ?> startStep = traversal.getStartStep();
        if (startStep instanceof StartStep && !startStep.getLabels().isEmpty()) {
            if (startStep.getLabels().size() > 1)
                throw new IllegalArgumentException("The start step of a match()-traversal can only have one label: " + startStep);
            TraversalHelper.replaceStep(traversal.getStartStep(), new SelectOneStep<>(traversal, Scope.global, Pop.head, startStep.getLabels().iterator().next()), traversal);
        }
        // END STEP to XMatchStep
        final Step<?, ?> endStep = traversal.getEndStep();
        if (!(endStep instanceof XMatchStep.XMatchEndStep)) {
            if (endStep.getLabels().size() > 1)
                throw new IllegalArgumentException("The end step of a match()-traversal can have at most one label: " + endStep);
            final String label = endStep.getLabels().size() == 0 ? null : endStep.getLabels().iterator().next();
            if (null != label) endStep.removeLabel(label);
            final Step<?, ?> xMatchEndStep = new XMatchStep.XMatchEndStep(traversal, matchStep, label);
            if (null != label) xMatchEndStep.addLabel(label);
            traversal.asAdmin().addStep(xMatchEndStep);
        }


        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof TraversalParent && !(step instanceof XMatchStep)) {
                ((TraversalParent) step).getGlobalChildren().forEach(t -> MatchStartEndStrategy.addSelectOneStartStep(matchStep, t));
                ((TraversalParent) step).getLocalChildren().forEach(t -> MatchStartEndStrategy.addSelectOneStartStep(matchStep, t));

            }
        }
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(XMatchStep.class, traversal).forEach(matchStep -> ((XMatchStep<?>) matchStep).getGlobalChildren().forEach(t -> MatchStartEndStrategy.addSelectOneStartStep(matchStep, t)));
    }

    @Override
    public Set<Class<? extends DecorationStrategy>> applyPrior() {
        return PRIORS;
    }

    public static MatchStartEndStrategy instance() {
        return INSTANCE;
    }
}