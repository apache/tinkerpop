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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SideEffectHelper {

    private SideEffectHelper() {
    }

    public static void validateSideEffectKey(final String key) throws IllegalArgumentException {
        if (null == key)
            throw TraversalSideEffects.Exceptions.sideEffectKeyCanNotBeNull();
        if (key.isEmpty())
            throw TraversalSideEffects.Exceptions.sideEffectKeyCanNotBeEmpty();
    }

    public static void validateSideEffectValue(final Object value) throws IllegalArgumentException {
        if (null == value)
            throw TraversalSideEffects.Exceptions.sideEffectValueCanNotBeNull();
    }

    public static void validateSideEffectKeyValue(final String key, final Object value) throws IllegalArgumentException {
        SideEffectHelper.validateSideEffectKey(key);
        SideEffectHelper.validateSideEffectValue(value);
    }

    public static Set<String> determineFrozenStepIds(final Traversal.Admin<?, ?> traversal) {
        final Set<String> frozenStepIds = new HashSet<>();
        if (!TraversalHelper.onGraphComputer(traversal)) {
            final Map<String, LinkedList<List<String>>> frozenStepIdMap = new HashMap<>();
            processSideEffectSteps(new HashSet<>(), frozenStepIdMap, traversal);
            for (final LinkedList<List<String>> list : frozenStepIdMap.values()) {
                list.removeLast();
                list.forEach(frozenStepIds::addAll);
            }
        }
        return frozenStepIds;
    }

    private static void processSideEffectSteps(final Set<String> activeSideEffectKeys,
                                               final Map<String, LinkedList<List<String>>> frozenStepIdMap,
                                               final Traversal.Admin<?, ?> traversal) {
        for (final Step step : traversal.getSteps()) {
            if (step instanceof SideEffectCapable && !(step instanceof Barrier)) {
                final String sideEffectKey = ((SideEffectCapable) step).getSideEffectKey();
                if (!frozenStepIdMap.containsKey(sideEffectKey)) {
                    frozenStepIdMap.put(sideEffectKey, new LinkedList<>());
                    frozenStepIdMap.get(sideEffectKey).add(new ArrayList<>());
                }
                activeSideEffectKeys.add(sideEffectKey);
                continue;
            }
            if (step instanceof Barrier) {
                activeSideEffectKeys.clear();
                for (final LinkedList<List<String>> list : frozenStepIdMap.values()) {
                    if (!list.getLast().isEmpty()) {
                        list.add(new ArrayList<>());
                    }
                }
            }
            if (activeSideEffectKeys.isEmpty()) {
                continue;
            }
            for (final String key : activeSideEffectKeys) {
                frozenStepIdMap.get(key).getLast().add(step.getId());
            }
            if (step instanceof Scoping && step.getRequirements().contains(TraverserRequirement.SIDE_EFFECTS)) {
                for (final String key : ((Scoping) step).getScopeKeys()) {
                    if (frozenStepIdMap.containsKey(key) && activeSideEffectKeys.contains(key)) {
                        frozenStepIdMap.get(key).add(new ArrayList<>());
                    }
                }
            } else if (step instanceof LambdaHolder) {
                for (final LinkedList<List<String>> list : frozenStepIdMap.values()) {
                    list.add(new ArrayList<>());
                }
            } else if (step instanceof TraversalParent) {
                final TraversalParent traversalParent = (TraversalParent) step;
                traversalParent.getGlobalChildren().forEach(t -> processSideEffectSteps(activeSideEffectKeys, frozenStepIdMap, t));
                traversalParent.getLocalChildren().forEach(t -> processSideEffectSteps(activeSideEffectKeys, frozenStepIdMap, t));
            }
        }
    }
}
