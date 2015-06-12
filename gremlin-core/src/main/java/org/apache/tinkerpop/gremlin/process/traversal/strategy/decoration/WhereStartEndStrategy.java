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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.ScopeP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WhereStartEndStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    private static final WhereStartEndStrategy INSTANCE = new WhereStartEndStrategy();

    private static final Set<Class<? extends DecorationStrategy>> PRIORS = new HashSet<>();

    static {
        PRIORS.add(ConjunctionStrategy.class);
    }


    private WhereStartEndStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        Step<?, ?> whereStep = traversal.getParent().asStep();
        while (!(whereStep instanceof EmptyStep || whereStep instanceof WhereStep)) {
            whereStep = whereStep.getTraversal().getParent().asStep();
        }

        if (whereStep instanceof WhereStep) {
            final Step<?, ?> startStep = traversal.getStartStep();
            if (startStep instanceof StartStep && !startStep.getLabels().isEmpty()) {
                if (startStep.getLabels().size() > 1)
                    throw new IllegalArgumentException("The start step of a where()-traversal predicate can only have one label: " + startStep);
                TraversalHelper.replaceStep(traversal.getStartStep(), new SelectOneStep<>(traversal, ((WhereStep) whereStep).getScope(), Pop.head, startStep.getLabels().iterator().next()), traversal);
            }
            //// END STEP
            final Step<?, ?> endStep = traversal.getEndStep();
            if (!endStep.getLabels().isEmpty()) {
                if (endStep.getLabels().size() > 1)
                    throw new IllegalArgumentException("The end step of a where()-traversal predicate can only have one label: " + endStep);
                final String label = endStep.getLabels().iterator().next();
                endStep.removeLabel(label);
                final IsStep<?> isStep = new IsStep<>(traversal, new ScopeP<>(P.eq(label)));
                traversal.addStep(isStep);
            }
        }
    }

    public static void getScopeP(final List<IsStep<?>> list, final TraversalParent traversalParent) {
        traversalParent.getLocalChildren().forEach(traversal -> {
            if (traversal.getEndStep() instanceof IsStep && ((IsStep) traversal.getEndStep()).getPredicate() instanceof ScopeP) {
                list.add((IsStep) traversal.getEndStep());
            }
            for (final Step<?, ?> step : traversal.getSteps()) {
                if (step instanceof TraversalParent)
                    getScopeP(list, (TraversalParent) step);
            }

        });
    }

    @Override
    public Set<Class<? extends DecorationStrategy>> applyPrior() {
        return PRIORS;
    }

    public static WhereStartEndStrategy instance() {
        return INSTANCE;
    }
}