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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.TreeSupplier;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TreeStep<S> extends ReducingBarrierStep<S, Tree> implements TraversalParent, ByModulating, PathProcessor {

    private TraversalRing<Object, Object> traversalRing = new TraversalRing<>();
    private Set<String> keepLabels;

    public TreeStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier((Supplier) TreeSupplier.instance());
        this.setReducingBiOperator(TreeBiOperator.instance());
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
    public void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        this.traversalRing.replaceTraversal(
                (Traversal.Admin<Object, Object>) oldTraversal,
                (Traversal.Admin<Object, Object>) newTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public Tree projectTraverser(final Traverser.Admin<S> traverser) {
        final Tree topTree = new Tree();
        Tree depth = topTree;
        final Path path = traverser.path();
        for (int i = 0; i < path.size(); i++) {
            final Object object = TraversalUtil.applyNullable(path.<Object>get(i), this.traversalRing.next());
            if (!depth.containsKey(object))
                depth.put(object, new Tree<>());
            depth = (Tree) depth.get(object);
        }
        this.traversalRing.reset();
        return topTree;
    }

    @Override
    public TreeStep<S> clone() {
        final TreeStep<S> clone = (TreeStep<S>) super.clone();
        clone.traversalRing = this.traversalRing.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.traversalRing.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.traversalRing.hashCode();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.traversalRing);
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public void setKeepLabels(final Set<String> keepLabels) {
        this.keepLabels = new HashSet<>(keepLabels);
    }

    @Override
    public Set<String> getKeepLabels() { return this.keepLabels; }

    ///////////

    public static final class TreeBiOperator implements BinaryOperator<Tree>, Serializable {

        private static final TreeBiOperator INSTANCE = new TreeBiOperator();

        @Override
        public Tree apply(final Tree mutatingSeed, final Tree tree) {
            mutatingSeed.addTree(tree);
            return mutatingSeed;
        }

        public static final TreeBiOperator instance() {
            return INSTANCE;
        }
    }
}