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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.util.SupplyingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectRegistrar;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SideEffectCapStep<S, E> extends SupplyingBarrierStep<S, E> implements SideEffectRegistrar {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(
            TraverserRequirement.SIDE_EFFECTS,
            TraverserRequirement.OBJECT
    );

    private List<String> sideEffectKeys;

    public SideEffectCapStep(final Traversal.Admin traversal, final String... sideEffectKeys) {
        super(traversal);
        this.sideEffectKeys = Arrays.asList(sideEffectKeys);
    }

    public void registerSideEffects() {
        if (this.sideEffectKeys.isEmpty())
            this.sideEffectKeys = Collections.singletonList(((SideEffectCapable) this.getPreviousStep()).getSideEffectKey());
        SideEffectCapStep.generateSupplier(this);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKeys);
    }

    public List<String> getSideEffectKeys() {
        return this.sideEffectKeys;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public SideEffectCapStep<S, E> clone() throws CloneNotSupportedException {
        final SideEffectCapStep<S, E> clone = (SideEffectCapStep<S, E>) super.clone();
        SideEffectCapStep.generateSupplier(clone);
        return clone;
    }

    public Map<String, Object> getMapOfSideEffects() {
        final Map<String, Object> sideEffects = new HashMap<>();
        for (final String sideEffectKey : this.sideEffectKeys) {
            sideEffects.put(sideEffectKey, this.getTraversal().asAdmin().getSideEffects().get(sideEffectKey));
        }
        return sideEffects;
    }

    /////////////////////////

    private static final <S, E> void generateSupplier(final SideEffectCapStep<S, E> sideEffectCapStep) {
        sideEffectCapStep.setSupplier(() -> sideEffectCapStep.sideEffectKeys.size() == 1 ?
                sideEffectCapStep.getTraversal().asAdmin().getSideEffects().get(sideEffectCapStep.sideEffectKeys.get(0)) :
                (E) sideEffectCapStep.getMapOfSideEffects());
    }
}
