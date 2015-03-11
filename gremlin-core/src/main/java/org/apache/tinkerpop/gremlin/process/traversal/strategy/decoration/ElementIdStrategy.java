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

import org.apache.tinkerpop.gremlin.process.T;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.graph.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Contains;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Arrays;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ElementIdStrategy extends AbstractTraversalStrategy {

    private final String idPropertyKey;

    public ElementIdStrategy(final String idPropertyKey) {
        this.idPropertyKey = idPropertyKey;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfAssignableClass(HasStep.class, traversal).stream()
                .filter(hasStep -> ((HasStep<?>) hasStep).getHasContainers().get(0).key.equals(T.id.getAccessor()))
                .forEach(hasStep -> ((HasStep<?>) hasStep).getHasContainers().get(0).key = this.idPropertyKey);

        if (traversal.getStartStep() instanceof GraphStep) {
            final GraphStep graphStep = (GraphStep) traversal.getStartStep();
            if (graphStep instanceof HasContainerHolder)
                ((HasContainerHolder) graphStep).addHasContainer(new HasContainer(this.idPropertyKey, Contains.within, Arrays.asList(graphStep.getIds())));
            else
                TraversalHelper.insertAfterStep(new HasStep(traversal, new HasContainer(this.idPropertyKey, Contains.within, Arrays.asList(graphStep.getIds()))), graphStep, traversal);
            graphStep.clearIds();
        }


    }

    @Override
    public String toString() {
        return StringFactory.traversalStrategyString(this);
    }
}
