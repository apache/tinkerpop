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

package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SelectLinearizationStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final SelectLinearizationStrategy INSTANCE = new SelectLinearizationStrategy();

    private SelectLinearizationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!TraversalHelper.onGraphComputer(traversal) || traversal.getParent().isLocalChild(traversal))
            return;

        final List<SelectOneStep> selectOneSteps = TraversalHelper.getStepsOfClass(SelectOneStep.class, traversal);
        for (final SelectOneStep<?, ?> selectOneStep : selectOneSteps) {
            if (selectOneStep.getMaxRequirement().compareTo(PathProcessor.ElementRequirement.ID) > 0) {
                final int index = TraversalHelper.stepIndex(selectOneStep, traversal);
                final Traversal.Admin<?, ?> localChild = selectOneStep.getLocalChildren().get(0);
                if (localChild instanceof ElementValueTraversal) { // need to generalize to get more coverage -- unfold()-style selects break.
                    selectOneStep.removeLocalChild(localChild);
                    final TraversalMapStep<?, ?> mapStep = new TraversalMapStep<>(traversal, localChild.clone());
                    traversal.addStep(index + 1, mapStep);
                }
            }
        }
    }

    public static SelectLinearizationStrategy instance() {
        return INSTANCE;
    }
}
