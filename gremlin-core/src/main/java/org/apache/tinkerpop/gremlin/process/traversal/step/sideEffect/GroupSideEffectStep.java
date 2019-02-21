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

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.ProfilingAware;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupSideEffectStep<S, K, V> extends SideEffectStep<S> implements SideEffectCapable<Map<K, ?>, Map<K, V>>, TraversalParent, ByModulating, ProfilingAware {

    private char state = 'k';
    private Traversal.Admin<S, K> keyTraversal;
    private Traversal.Admin<S, V> valueTraversal;
    private Barrier barrierStep;
    private boolean resetBarrierForProfiling = false;
    ///
    private String sideEffectKey;

    public GroupSideEffectStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.valueTraversal = this.integrateChild(__.fold().asAdmin());
        this.barrierStep = GroupStep.determineBarrierStep(this.valueTraversal);
        this.getTraversal().getSideEffects().registerIfAbsent(this.sideEffectKey, HashMapSupplier.instance(),
                new GroupStep.GroupBiOperator<>(null == this.barrierStep ?
                        Operator.assign :
                        this.barrierStep.getMemoryComputeKey().getReducer()));
    }

    /**
     * Reset the {@link Barrier} on the step to be wrapped in a {@link ProfilingAware.ProfiledBarrier} which can
     * properly start/stop the timer on the associated {@link ProfileStep}.
     */
    @Override
    public void prepareForProfiling() {
        resetBarrierForProfiling = barrierStep != null;
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> kvTraversal) {
        if ('k' == this.state) {
            this.keyTraversal = this.integrateChild(kvTraversal);
            this.state = 'v';
        } else if ('v' == this.state) {
            this.valueTraversal = this.integrateChild(GroupStep.convertValueTraversal(kvTraversal));
            this.barrierStep = GroupStep.determineBarrierStep(this.valueTraversal);
            this.getTraversal().getSideEffects().register(this.sideEffectKey, null,
                    new GroupStep.GroupBiOperator<>(null == this.barrierStep ?
                            Operator.assign :
                            this.barrierStep.getMemoryComputeKey().getReducer()));
            this.state = 'x';
        } else {
            throw new IllegalStateException("The key and value traversals for group()-step have already been set: " + this);
        }
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final Map<K, V> map = new HashMap<>(1);
        this.valueTraversal.reset();
        this.valueTraversal.addStart(traverser);

        // reset the barrierStep as there are now ProfileStep instances present and the timers won't start right
        // without specific configuration through wrapping both the Barrier and ProfileStep in ProfiledBarrier
        if (resetBarrierForProfiling) barrierStep = GroupStep.determineBarrierStep(valueTraversal);

        if (null == this.barrierStep) {
            if (this.valueTraversal.hasNext())
                map.put(TraversalUtil.applyNullable(traverser, this.keyTraversal), (V) this.valueTraversal.next());
        } else if (this.barrierStep.hasNextBarrier())
            map.put(TraversalUtil.applyNullable(traverser, this.keyTraversal), (V) this.barrierStep.nextBarrier());
        if (!map.isEmpty())
            this.getTraversal().getSideEffects().add(this.sideEffectKey, map);
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.sideEffectKey, this.keyTraversal, this.valueTraversal);
    }

    @Override
    public List<Traversal.Admin<?, ?>> getLocalChildren() {
        final List<Traversal.Admin<?, ?>> children = new ArrayList<>(2);
        if (null != this.keyTraversal)
            children.add(this.keyTraversal);
        children.add(this.valueTraversal);
        return children;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT, TraverserRequirement.BULK, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public GroupSideEffectStep<S, K, V> clone() {
        final GroupSideEffectStep<S, K, V> clone = (GroupSideEffectStep<S, K, V>) super.clone();
        if (null != this.keyTraversal)
            clone.keyTraversal = this.keyTraversal.clone();
        clone.valueTraversal = this.valueTraversal.clone();
        clone.barrierStep = GroupStep.determineBarrierStep(clone.valueTraversal);
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.keyTraversal);
        this.integrateChild(this.valueTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.sideEffectKey.hashCode();
        if (this.keyTraversal != null) result ^= this.keyTraversal.hashCode();
        result ^= this.valueTraversal.hashCode();
        return result;
    }

    @Override
    public Map<K, V> generateFinalResult(final Map<K, ?> object) {
        return GroupStep.doFinalReduction((Map<K, Object>) object, this.valueTraversal);
    }
}