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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * OrderGlobalRemovalStrategy will remove all {@link OrderGlobalStep} instances in an {@link Traversal} as long as it is not the final, end-step.
 * This strategy is only applied when the {@link org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine} is a {@link org.apache.tinkerpop.gremlin.process.traversal.engine.ComputerTraversalEngine}.
 * Mid-traversal ordering does not make sense in a distributed, parallel computation as at the next step, everything is executed in parallel again (thus, loosing the order).
 * However, if the last step is an {@link OrderGlobalStep}, then the result set is ordered accordingly.
 * <p/>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @example <pre>
 * __.order().out()             // is replaced by out() (OLAP only)
 * __.out().order().by("name")  // is not replaced
 * </pre>
 */
public final class OrderGlobalRemovalStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final OrderGlobalRemovalStrategy INSTANCE = new OrderGlobalRemovalStrategy();

    private OrderGlobalRemovalStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getEngine().isStandard())
            return;

        if (TraversalHelper.hasStepOfAssignableClass(OrderGlobalStep.class, traversal)) {
            final Step endStep = traversal.getEndStep();
            TraversalHelper.getStepsOfAssignableClass(OrderGlobalStep.class, traversal)
                    .stream()
                    .filter(step -> step != endStep)
                    .forEach(step -> traversal.removeStep((Step) step));
        }
    }

    public static OrderGlobalRemovalStrategy instance() {
        return INSTANCE;
    }
}
