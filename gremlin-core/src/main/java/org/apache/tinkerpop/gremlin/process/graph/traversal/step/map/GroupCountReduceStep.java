/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Reducing;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.util.MapHelper;

import java.util.*;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class GroupCountReduceStep<S> extends ReducingBarrierStep<S, Map<Object, Long>> implements Reducing<Map<Object, Long>, Traverser<S>>, TraversalParent {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(TraverserRequirement.BULK);
    private Traversal.Admin<S, Object> groupTraversal = new IdentityTraversal<>();

    public GroupCountReduceStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier(HashMap::new);
        this.setBiFunction((map, traverser) -> {
            MapHelper.incr(map, TraversalUtil.apply(traverser.asAdmin(), this.groupTraversal), traverser.bulk());
            return map;
        });
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
    public Reducer<Map<Object, Long>, Traverser<S>> getReducer() {
        return new Reducer<>(this.getSeedSupplier(), this.getBiFunction(), true);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public GroupCountReduceStep<S> clone() throws CloneNotSupportedException {
        final GroupCountReduceStep<S> clone = (GroupCountReduceStep<S>) super.clone();
        clone.groupTraversal = this.integrateChild(this.groupTraversal.clone(), TYPICAL_LOCAL_OPERATIONS);
        return clone;
    }
}