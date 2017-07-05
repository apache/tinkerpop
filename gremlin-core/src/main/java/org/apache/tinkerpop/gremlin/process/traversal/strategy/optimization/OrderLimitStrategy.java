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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SackStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderLimitStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final OrderLimitStrategy INSTANCE = new OrderLimitStrategy();

    private static Set<Class<? extends Step>> LEGAL_STEPS = new HashSet<>(
            Arrays.asList(LabelStep.class,
                    IdStep.class,
                    PathStep.class,
                    SelectStep.class,
                    SelectOneStep.class,
                    SackStep.class,
                    TreeStep.class));

    private OrderLimitStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!TraversalHelper.onGraphComputer(traversal))
            return;

        final List<OrderGlobalStep> orders = TraversalHelper.getStepsOfClass(OrderGlobalStep.class, traversal);
        for (final OrderGlobalStep order : orders) {
            RangeGlobalStep range = null;
            Step<?, ?> currentStep = order.getNextStep();
            while (true) {
                if (currentStep instanceof RangeGlobalStep) {
                    range = (RangeGlobalStep) currentStep;
                    break;
                } else if (!LEGAL_STEPS.contains(currentStep.getClass()))
                    break;
                else
                    currentStep = currentStep.getNextStep();
            }
            if (null != range)
                order.setLimit(range.getHighRange());
        }
    }

    public static OrderLimitStrategy instance() {
        return INSTANCE;
    }
}
