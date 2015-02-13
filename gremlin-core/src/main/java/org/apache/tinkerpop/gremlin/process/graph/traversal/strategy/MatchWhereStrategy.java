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
package org.apache.tinkerpop.gremlin.process.graph.traversal.strategy;

import org.apache.tinkerpop.gremlin.process.Step;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter.WhereStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.match.MatchStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MatchWhereStrategy extends AbstractTraversalStrategy implements TraversalStrategy {

    private static final MatchWhereStrategy INSTANCE = new MatchWhereStrategy();
    private static final Set<Class<? extends TraversalStrategy>> PRIORS = new HashSet<>();

    static {
        PRIORS.add(IdentityRemovalStrategy.class);
    }

    private MatchWhereStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!TraversalHelper.hasStepOfClass(MatchStep.class, traversal))
            return;

        final List<MatchStep> matchSteps = TraversalHelper.getStepsOfClass(MatchStep.class, traversal);
        for (final MatchStep matchStep : matchSteps) {
            boolean foundWhereWithNoTraversal = false;
            Step currentStep = matchStep.getNextStep();
            while (currentStep instanceof WhereStep || currentStep instanceof SelectStep || currentStep instanceof SelectOneStep || currentStep instanceof IdentityStep) {
                if (currentStep instanceof WhereStep) {
                    if (!((WhereStep) currentStep).getLocalChildren().isEmpty()) {
                        matchStep.addTraversal(((WhereStep<?>) currentStep).getLocalChildren().get(0));
                        traversal.removeStep(currentStep);
                    } else {
                        foundWhereWithNoTraversal = true;
                    }
                } else if (currentStep instanceof SelectStep) {
                    if (!((SelectStep) currentStep).getLocalChildren().isEmpty() || foundWhereWithNoTraversal)
                        break;
                } else if (currentStep instanceof SelectOneStep) {
                    if (!((SelectOneStep) currentStep).getLocalChildren().isEmpty() || foundWhereWithNoTraversal)
                        break;
                }
                // else is the identity step
                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static MatchWhereStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPrior() {
        return PRIORS;
    }

}
