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
package com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.traversal.step.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.mapreduce.GroupCountMapReduce;
import com.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import com.tinkerpop.gremlin.process.traversal.step.MapReducer;
import com.tinkerpop.gremlin.process.traversal.step.Reversible;
import com.tinkerpop.gremlin.process.traversal.step.SideEffectRegistrar;
import com.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.MapHelper;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupCountStep<S> extends SideEffectStep<S> implements SideEffectRegistrar, SideEffectCapable, Reversible, TraversalParent, MapReducer<Object, Long, Object, Long, Map<Object, Long>> {

    private Traversal.Admin<S, Object> groupTraversal = new IdentityTraversal<>();
    private String sideEffectKey;
    // TODO: onFirst like subgraph so we don't keep getting the map

    public GroupCountStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final Map<Object, Long> groupCountMap = traverser.sideEffects(this.sideEffectKey);
        MapHelper.incr(groupCountMap, TraversalUtil.apply(traverser.asAdmin(), this.groupTraversal), traverser.bulk());
    }

    @Override
    public void registerSideEffects() {
        if (this.sideEffectKey == null) this.sideEffectKey = this.getId();
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, HashMap<Object, Long>::new);
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public MapReduce<Object, Long, Object, Long, Map<Object, Long>> getMapReduce() {
        return new GroupCountMapReduce(this);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey, this.groupTraversal);
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> traversal) {
        this.groupTraversal = this.integrateChild(traversal, TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    public List<Traversal.Admin<S, Object>> getLocalChildren() {
        return Collections.singletonList(this.groupTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.BULK, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public GroupCountStep<S> clone() throws CloneNotSupportedException {
        final GroupCountStep<S> clone = (GroupCountStep<S>) super.clone();
        clone.groupTraversal = this.integrateChild(this.groupTraversal.clone(), TYPICAL_LOCAL_OPERATIONS);
        return clone;
    }
}
