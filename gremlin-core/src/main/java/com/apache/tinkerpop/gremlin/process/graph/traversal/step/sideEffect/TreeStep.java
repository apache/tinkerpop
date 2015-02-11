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

import com.apache.tinkerpop.gremlin.process.Path;
import com.apache.tinkerpop.gremlin.process.Traversal;
import com.apache.tinkerpop.gremlin.process.computer.MapReduce;
import com.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import com.apache.tinkerpop.gremlin.process.traversal.step.Reversible;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.SideEffectCapable;
import com.apache.tinkerpop.gremlin.process.traversal.step.SideEffectRegistrar;
import com.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.mapreduce.TreeMapReduce;
import com.apache.tinkerpop.gremlin.process.graph.util.Tree;
import com.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import com.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import com.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TreeStep<S> extends SideEffectStep<S> implements SideEffectRegistrar, Reversible, SideEffectCapable, TraversalParent, MapReducer<Object, Tree, Object, Tree, Tree> {

    private TraversalRing<Object, Object> traversalRing;
    private String sideEffectKey;

    public TreeStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.traversalRing = new TraversalRing<>();
        TreeStep.generateConsumer(this);
    }

    @Override
    public void registerSideEffects() {
        if (null == this.sideEffectKey) this.sideEffectKey = this.getId();
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, Tree::new);
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public MapReduce<Object, Tree, Object, Tree, Tree> getMapReduce() {
        return new TreeMapReduce(this);
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey, this.traversalRing);
    }

    @Override
    public TreeStep<S> clone() throws CloneNotSupportedException {
        final TreeStep<S> clone = (TreeStep<S>) super.clone();
        clone.traversalRing = new TraversalRing<>();
        for (final Traversal.Admin<Object, Object> traversal : this.traversalRing.getTraversals()) {
            clone.traversalRing.addTraversal(clone.integrateChild(traversal.clone(), TYPICAL_LOCAL_OPERATIONS));
        }
        TreeStep.generateConsumer(clone);
        return clone;
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getLocalChildren() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> treeTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(treeTraversal, TYPICAL_LOCAL_OPERATIONS));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    /////////////////////////

    private static final <S> void generateConsumer(final TreeStep<S> treeStep) {
        treeStep.setConsumer(traverser -> {
            Tree depth = traverser.sideEffects(treeStep.sideEffectKey);
            final Path path = traverser.path();
            for (int i = 0; i < path.size(); i++) {
                final Object object = TraversalUtil.apply(path.<Object>get(i), treeStep.traversalRing.next());
                if (!depth.containsKey(object))
                    depth.put(object, new Tree<>());
                depth = (Tree) depth.get(object);
            }
            treeStep.traversalRing.reset();
        });
    }
}
