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

package org.apache.tinkerpop.gremlin.process.actor.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ActorVerificationStrategy extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy> implements TraversalStrategy.VerificationStrategy {

    private static final ActorVerificationStrategy INSTANCE = new ActorVerificationStrategy();

    private ActorVerificationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!TraversalHelper.getStepsOfAssignableClass(InjectStep.class, traversal).isEmpty())
            throw new VerificationException("Inject traversal currently not supported", traversal);


        final boolean globalChild = TraversalHelper.isGlobalChild(traversal);
        for (final Step<?, ?> step : traversal.getSteps()) {
            // only global children are graph computing
            if (globalChild && step instanceof GraphComputing)
                ((GraphComputing) step).onGraphComputer();

            for (String label : step.getLabels()) {
                if (Graph.Hidden.isHidden(label))
                    step.removeLabel(label);
            }
        }
    }

    public static ActorVerificationStrategy instance() {
        return INSTANCE;
    }
}
