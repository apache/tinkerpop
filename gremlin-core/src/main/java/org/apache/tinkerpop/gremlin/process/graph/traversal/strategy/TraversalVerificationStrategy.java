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
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.util.SupplyingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalVerificationStrategy extends AbstractTraversalStrategy {

    private static final TraversalVerificationStrategy INSTANCE = new TraversalVerificationStrategy();

    private TraversalVerificationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (engine.isStandard())
            return;

        Step<?, ?> endStep = traversal.getEndStep() instanceof ComputerAwareStep.EndStep ?
                ((ComputerAwareStep.EndStep) traversal.getEndStep()).getPreviousStep() :
                traversal.getEndStep();
        if(endStep instanceof ComputerResultStep)
            endStep = endStep.getPreviousStep();

        for (final Step<?, ?> step : traversal.getSteps()) {
            if ((step instanceof ReducingBarrierStep || step instanceof SupplyingBarrierStep) && (step != endStep || !(traversal.getParent() instanceof EmptyStep))) {
                throw new IllegalStateException("Global traversals on GraphComputer may not contain mid-traversal barriers: " + step);
            }
            if (step instanceof TraversalParent) {
                final Optional<Traversal.Admin<Object, Object>> traversalOptional = ((TraversalParent) step).getLocalChildren().stream()
                        .filter(t -> !TraversalHelper.isLocalStarGraph(t.asAdmin()))
                        .findAny();
                if (traversalOptional.isPresent())
                    throw new IllegalStateException("Local traversals on GraphComputer may not traverse past the local star-graph: " + traversalOptional.get());
            }
        }
    }

    public static TraversalVerificationStrategy instance() {
        return INSTANCE;
    }
}
