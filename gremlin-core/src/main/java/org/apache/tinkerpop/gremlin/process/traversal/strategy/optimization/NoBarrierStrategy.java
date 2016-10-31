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
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class NoBarrierStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy>
        implements TraversalStrategy.OptimizationStrategy {

    static final String LAZY_STEPS_METADATA_KEY = "gremlin.noBarrier.lazySteps";
    private static final NoBarrierStrategy INSTANCE = new NoBarrierStrategy();
    private final Set<Class<? extends OptimizationStrategy>> postStrategies;

    private NoBarrierStrategy() {
        postStrategies = new HashSet<>(2);
        postStrategies.add(LazyBarrierStrategy.class);
        postStrategies.add(PathRetractionStrategy.class);
    }

    private static void processSideEffectSteps(final Set<String> activeSideEffectKeys,
                                               final Map<String, LinkedList<List<String>>> lazyStepIdMap,
                                               final Traversal.Admin<?, ?> traversal) {
        for (final Step step : traversal.getSteps()) {
            if (step instanceof SideEffectCapable && !(step instanceof Barrier)) {
                final String sideEffectKey = ((SideEffectCapable) step).getSideEffectKey();
                if (!lazyStepIdMap.containsKey(sideEffectKey)) {
                    lazyStepIdMap.put(sideEffectKey, new LinkedList<>());
                    lazyStepIdMap.get(sideEffectKey).add(new ArrayList<>());
                }
                activeSideEffectKeys.add(sideEffectKey);
                continue;
            }
            if (step instanceof Barrier) {
                activeSideEffectKeys.clear();
                for (final LinkedList<List<String>> list : lazyStepIdMap.values()) {
                    if (!list.getLast().isEmpty()) {
                        list.add(new ArrayList<>());
                    }
                }
            }
            if (activeSideEffectKeys.isEmpty()) {
                continue;
            }
            for (final String key : activeSideEffectKeys) {
                lazyStepIdMap.get(key).getLast().add(step.getId());
            }
            if (step instanceof Scoping && step.getRequirements().contains(TraverserRequirement.SIDE_EFFECTS)) {
                for (final String key : ((Scoping) step).getScopeKeys()) {
                    if (lazyStepIdMap.containsKey(key) && activeSideEffectKeys.contains(key)) {
                        lazyStepIdMap.get(key).add(new ArrayList<>());
                    }
                }
            } else if (step instanceof LambdaHolder) {
                for (final LinkedList<List<String>> list : lazyStepIdMap.values()) {
                    list.add(new ArrayList<>());
                }
            } else if (step instanceof TraversalParent) {
                final TraversalParent traversalParent = (TraversalParent) step;
                traversalParent.getGlobalChildren().forEach(t -> processSideEffectSteps(activeSideEffectKeys, lazyStepIdMap, t));
                traversalParent.getLocalChildren().forEach(t -> processSideEffectSteps(activeSideEffectKeys, lazyStepIdMap, t));
            }
        }
    }

    public static NoBarrierStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getParent() instanceof EmptyStep && !TraversalHelper.onGraphComputer(traversal)) {
            final Set<String> lazyStepIds = new HashSet<>();
            if (!TraversalHelper.onGraphComputer(traversal)) {
                final Map<String, LinkedList<List<String>>> lazyStepIdMap = new HashMap<>();
                processSideEffectSteps(new HashSet<>(), lazyStepIdMap, traversal);
                for (final LinkedList<List<String>> list : lazyStepIdMap.values()) {
                    list.removeLast();
                    list.forEach(lazyStepIds::addAll);
                }
            }
            traversal.setMetadata(LAZY_STEPS_METADATA_KEY, lazyStepIds);
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPost() {
        return postStrategies;
    }
}
