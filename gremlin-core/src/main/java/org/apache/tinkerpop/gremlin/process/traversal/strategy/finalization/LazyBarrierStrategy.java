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

package org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @deprecated As of release 3.2.3. Please use {@link org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy}.
 */
@Deprecated
public final class LazyBarrierStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy> implements TraversalStrategy.FinalizationStrategy {

    private static final LazyBarrierStrategy INSTANCE = new LazyBarrierStrategy();
    private static final Set<Class<? extends FinalizationStrategy>> PRIORS = new HashSet<>();
    private static final Set<Class<? extends FinalizationStrategy>> POSTS = new HashSet<>();

    private static final int REQUIRED_DEPTH = 2;
    private static final int BIG_START_SIZE = 5;
    protected static final int MAX_BARRIER_SIZE = 10000;

    static {
        POSTS.add(ProfileStrategy.class);
    }


    private LazyBarrierStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        if (traversal.getTraverserRequirements().contains(TraverserRequirement.PATH))
            return;

        int depth = 0;
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof VertexStep)
                depth++;
        }

        if (depth > REQUIRED_DEPTH) {
            boolean bigStart = false;
            char foundVertexStep = 'x';
            for (int i = 0; i < traversal.getSteps().size() - 1; i++) {
                final Step<?, ?> step = traversal.getSteps().get(i);
                if (i == 0)
                    bigStart = step instanceof GraphStep && (((GraphStep) step).getIds().length >= BIG_START_SIZE || (((GraphStep) step).getIds().length == 0 && step instanceof HasContainerHolder && ((HasContainerHolder) step).getHasContainers().isEmpty()));
                else if ('v' == foundVertexStep || bigStart) {
                    if (!(step instanceof FilterStep) && !(step instanceof Barrier) && !(step instanceof VertexStep && ((VertexStep) step).returnsEdge())) {
                        TraversalHelper.insertAfterStep(new NoOpBarrierStep<>(traversal, MAX_BARRIER_SIZE), step, traversal);
                    }
                }

                if ('x' == foundVertexStep && step instanceof VertexStep)
                    foundVertexStep = ((VertexStep) step).returnsVertex() ? 'v' : 'e';
                else if ('e' == foundVertexStep && step instanceof EdgeVertexStep)
                    foundVertexStep = 'v';
            }
        }
    }


    @Override
    public Set<Class<? extends FinalizationStrategy>> applyPrior() {
        return PRIORS;
    }

    @Override
    public Set<Class<? extends FinalizationStrategy>> applyPost() {
        return POSTS;
    }

    public static LazyBarrierStrategy instance() {
        return INSTANCE;
    }
}
