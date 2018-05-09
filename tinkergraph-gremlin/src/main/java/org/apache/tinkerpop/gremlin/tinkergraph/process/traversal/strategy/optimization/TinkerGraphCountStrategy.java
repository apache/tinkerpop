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

package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.map.TinkerCountGlobalStep;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * This strategy will do a direct {@link org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper#getVertices}
 * size call if the traversal is a count of the vertices and edges of the graph or a one-to-one map chain thereof.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @example <pre>
 * g.V().count()               // is replaced by TinkerCountGlobalStep
 * g.V().map(out()).count()    // is replaced by TinkerCountGlobalStep
 * g.E().label().count()       // is replaced by TinkerCountGlobalStep
 * </pre>
 */
public final class TinkerGraphCountStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final TinkerGraphCountStrategy INSTANCE = new TinkerGraphCountStrategy();

    private TinkerGraphCountStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.getParent() instanceof EmptyStep) || TraversalHelper.onGraphComputer(traversal))
            return;
        final List<Step> steps = traversal.getSteps();
        if (steps.size() < 2 ||
                !(steps.get(0) instanceof GraphStep) ||
                0 != ((GraphStep) steps.get(0)).getIds().length ||
                !(steps.get(steps.size() - 1) instanceof CountGlobalStep))
            return;
        for (int i = 1; i < steps.size() - 1; i++) {
            final Step current = steps.get(i);
            if (!(//current instanceof MapStep ||  // MapSteps will not necessarily emit an element as demonstrated in https://issues.apache.org/jira/browse/TINKERPOP-1958
                    current instanceof IdentityStep ||
                    current instanceof NoOpBarrierStep ||
                    current instanceof CollectingBarrierStep) ||
                    (current instanceof TraversalParent &&
                            TraversalHelper.anyStepRecursively(s -> (s instanceof SideEffectStep || s instanceof AggregateStep), (TraversalParent) current)))
                return;
        }
        final Class<? extends Element> elementClass = ((GraphStep<?, ?>) steps.get(0)).getReturnClass();
        TraversalHelper.removeAllSteps(traversal);
        traversal.addStep(new TinkerCountGlobalStep<>(traversal, elementClass));
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPost() {
        return Collections.singleton(TinkerGraphStepStrategy.class);
    }

    public static TinkerGraphCountStrategy instance() {
        return INSTANCE;
    }
}
