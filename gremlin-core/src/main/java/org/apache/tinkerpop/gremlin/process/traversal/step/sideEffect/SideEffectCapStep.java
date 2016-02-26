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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.SupplyingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SideEffectCapStep<S, E> extends SupplyingBarrierStep<S, E> implements GraphComputing<E> {

    private List<String> sideEffectKeys;
    private transient Map<String, GraphComputing> sideEffectFinalizer;
    private boolean onGraphComputer = false;

    public SideEffectCapStep(final Traversal.Admin traversal, final String sideEffectKey, final String... sideEffectKeys) {
        super(traversal);
        this.sideEffectKeys = new ArrayList<>(1 + sideEffectKeys.length);
        this.sideEffectKeys.add(sideEffectKey);
        for (final String key : sideEffectKeys) {
            this.sideEffectKeys.add(key);
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.sideEffectKeys);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        for (final String sideEffectKey : this.sideEffectKeys) {
            result ^= sideEffectKey.hashCode();
        }
        return result;
    }

    public List<String> getSideEffectKeys() {
        return this.sideEffectKeys;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    protected E supply() {
        if (this.onGraphComputer)
            return null;
        else
            return this.generateFinalResult(null);
    }

    private Map<String, Object> getMapOfSideEffects() {
        final Map<String, Object> sideEffects = new HashMap<>();
        for (final String sideEffectKey : this.sideEffectKeys) {
            this.getTraversal().asAdmin().getSideEffects().get(sideEffectKey).ifPresent(value -> {
                if (this.onGraphComputer) {
                    sideEffects.put(sideEffectKey, value);
                } else {
                    final GraphComputing finalizer = this.sideEffectFinalizer.get(sideEffectKey);
                    sideEffects.put(sideEffectKey, null == finalizer ? value : finalizer.generateFinalResult(value));
                }
            });
        }
        return sideEffects;
    }

    @Override
    public void onGraphComputer() {
        this.onGraphComputer = true;
    }

    @Override
    public Optional<MemoryComputeKey> getMemoryComputeKey() {
        return Optional.of(MemoryComputeKey.of(this.getId(), Operator.assign, false, true));
    }

    @Override
    public E generateFinalResult(final E nothing) {
        if (!this.onGraphComputer && null == this.sideEffectFinalizer) {
            this.sideEffectFinalizer = new HashMap<>();
            for (final String key : this.sideEffectKeys) {
                for (final GraphComputing<?> graphComputing : TraversalHelper.getStepsOfAssignableClassRecursively(GraphComputing.class, TraversalHelper.getRootTraversal(this.getTraversal()))) {
                    if (graphComputing.getMemoryComputeKey().isPresent() && graphComputing.getMemoryComputeKey().get().getKey().equals(key)) {
                        this.sideEffectFinalizer.put(key, graphComputing);
                    }
                }
            }
        }
        ////////////
        if (this.sideEffectKeys.size() == 1) {
            final String sideEffectKey = this.sideEffectKeys.get(0);
            final E result = this.getTraversal().getSideEffects().<E>get(sideEffectKey).get();
            if (this.onGraphComputer)
                return result;
            else {
                final GraphComputing finalizer = this.sideEffectFinalizer.get(sideEffectKey);
                return null == finalizer ? result : (E) finalizer.generateFinalResult(result);
            }
        } else
            return (E) this.getMapOfSideEffects();
    }
}
