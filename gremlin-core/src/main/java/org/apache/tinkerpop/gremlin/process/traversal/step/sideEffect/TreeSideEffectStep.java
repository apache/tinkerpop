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
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.TreeSupplier;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TreeSideEffectStep<S> extends SideEffectStep<S> implements SideEffectCapable, TraversalParent, ByModulating, PathProcessor, GraphComputing {

    private TraversalRing<Object, Object> traversalRing;
    private String sideEffectKey;

    public TreeSideEffectStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.traversalRing = new TraversalRing<>();
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, TreeSupplier.instance());
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        Tree depth = traverser.sideEffects(this.sideEffectKey);
        final Path path = traverser.path();
        for (int i = 0; i < path.size(); i++) {
            final Object object = TraversalUtil.apply(path.<Object>get(i), this.traversalRing.next());
            if (!depth.containsKey(object))
                depth.put(object, new Tree<>());
            depth = (Tree) depth.get(object);
        }
        this.traversalRing.reset();
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.sideEffectKey, this.traversalRing);
    }

    @Override
    public TreeSideEffectStep<S> clone() {
        final TreeSideEffectStep<S> clone = (TreeSideEffectStep<S>) super.clone();
        clone.traversalRing = this.traversalRing.clone();
        clone.getLocalChildren().forEach(clone::integrateChild);
        return clone;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.sideEffectKey.hashCode() ^ this.traversalRing.hashCode();
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getLocalChildren() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> treeTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(treeTraversal));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public void onGraphComputer() {

    }

    @Override
    public Optional<MemoryComputeKey> getMemoryComputeKey() {
        return Optional.of(MemoryComputeKey.of(this.getSideEffectKey(), TreeStep.TreeBiOperator.instance(), false, false));
    }
}
