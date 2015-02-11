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
package com.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import com.apache.tinkerpop.gremlin.process.Traversal;
import com.apache.tinkerpop.gremlin.process.computer.MapReduce;
import com.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import com.apache.tinkerpop.gremlin.process.traversal.step.Reversible;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.SideEffectCapable;
import com.apache.tinkerpop.gremlin.process.traversal.step.SideEffectRegistrar;
import com.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.mapreduce.AggregateMapReduce;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.util.CollectingBarrierStep;
import com.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import com.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import com.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.apache.tinkerpop.gremlin.process.util.BulkSet;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AggregateStep<S> extends CollectingBarrierStep<S> implements SideEffectRegistrar, SideEffectCapable, Reversible, TraversalParent, MapReducer<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> {

    private Traversal.Admin<S, Object> aggregateTraversal = new IdentityTraversal<>();
    private String sideEffectKey;

    public AggregateStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        AggregateStep.generateConsumer(this);
    }

    @Override
    public void registerSideEffects() {
        if (null == this.sideEffectKey) this.sideEffectKey = this.getId();
        this.getTraversal().asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, BulkSet::new);
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey, this.aggregateTraversal);
    }

    @Override
    public MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> getMapReduce() {
        return new AggregateMapReduce(this);
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> traversal) {
        this.aggregateTraversal = this.integrateChild(traversal, TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    public List<Traversal.Admin<S, Object>> getLocalChildren() {
        return Collections.singletonList(this.aggregateTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.BULK, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public AggregateStep<S> clone() throws CloneNotSupportedException {
        final AggregateStep<S> clone = (AggregateStep<S>) super.clone();
        clone.aggregateTraversal = this.integrateChild(this.aggregateTraversal.clone(), TYPICAL_LOCAL_OPERATIONS);
        AggregateStep.generateConsumer(clone);
        return clone;
    }

    /////////////////////////

    private static final <S> void generateConsumer(final AggregateStep<S> aggregateStep) {
        aggregateStep.setConsumer(traverserSet ->
                traverserSet.forEach(traverser ->
                        TraversalHelper.addToCollection(
                                traverser.getSideEffects().get(aggregateStep.sideEffectKey),
                                TraversalUtil.apply(traverser, aggregateStep.aggregateTraversal),
                                traverser.bulk())));
    }
}
