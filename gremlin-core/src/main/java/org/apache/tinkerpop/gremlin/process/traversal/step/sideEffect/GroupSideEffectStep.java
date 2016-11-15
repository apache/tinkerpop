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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
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
public final class GroupSideEffectStep<S, K, V> extends SideEffectStep<S> implements SideEffectCapable<Map<K, ?>, Map<K, V>>, TraversalParent, ByModulating {

    private char state = 'k';
    private Traversal.Admin<S, K> keyTraversal;
    private Traversal.Admin<S, ?> preTraversal;
    private Traversal.Admin<S, V> valueTraversal;
    ///
    private String sideEffectKey;

    public GroupSideEffectStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.valueTraversal = this.integrateChild(__.fold().asAdmin());
        this.preTraversal = this.integrateChild(GroupStep.generatePreTraversal(this.valueTraversal));
        this.getTraversal().getSideEffects().registerIfAbsent(this.sideEffectKey, HashMapSupplier.instance(), new GroupStep.GroupBiOperator<>(this.valueTraversal));
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> kvTraversal) {
        if ('k' == this.state) {
            this.keyTraversal = this.integrateChild(kvTraversal);
            this.state = 'v';
        } else if ('v' == this.state) {
            this.valueTraversal = this.integrateChild(GroupStep.convertValueTraversal(kvTraversal));
            this.preTraversal = this.integrateChild(GroupStep.generatePreTraversal(this.valueTraversal));
            this.getTraversal().getSideEffects().register(this.sideEffectKey, null, new GroupStep.GroupBiOperator<>(this.valueTraversal));
            this.state = 'x';
        } else {
            throw new IllegalStateException("The key and value traversals for group()-step have already been set: " + this);
        }
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final Map<K, V> map = new HashMap<>(1);
        if (null == this.preTraversal) {
            map.put(TraversalUtil.applyNullable(traverser, this.keyTraversal), (V) traverser.split());
        } else {
            final TraverserSet traverserSet = new TraverserSet<>();
            this.preTraversal.reset();
            this.preTraversal.addStart(traverser.split());
            while(this.preTraversal.hasNext()) {
                traverserSet.add(this.preTraversal.nextTraverser());
            }
            map.put(TraversalUtil.applyNullable(traverser, this.keyTraversal), (V) traverserSet);
        }
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
        final List<Traversal.Admin<?, ?>> children = new ArrayList<>(3);
        if (null != this.keyTraversal)
            children.add(this.keyTraversal);
        children.add(this.valueTraversal);
        if (null != this.preTraversal)
            children.add(this.preTraversal);
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
        clone.preTraversal = (Traversal.Admin<S, ?>) GroupStep.generatePreTraversal(clone.valueTraversal);
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.keyTraversal);
        this.integrateChild(this.valueTraversal);
        this.integrateChild(this.preTraversal);
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
