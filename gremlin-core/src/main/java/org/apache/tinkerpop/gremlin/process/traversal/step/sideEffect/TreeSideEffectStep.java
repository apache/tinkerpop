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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.traversal.VertexTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.TreeSupplier;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TreeSideEffectStep<S> extends SideEffectStep<S> implements SideEffectCapable, TraversalParent, MapReducer<Object, Tree, Object, Tree, Tree> {

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
    public MapReduce<Object, Tree, Object, Tree, Tree> getMapReduce() {
        return new TreeSideEffectMapReduce(this);
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
    public TreeSideEffectStep<S> clone()  {
        final TreeSideEffectStep<S> clone = (TreeSideEffectStep<S>) super.clone();
        clone.traversalRing = new TraversalRing<>();
        for (final Traversal.Admin<Object, Object> traversal : this.traversalRing.getTraversals()) {
            clone.traversalRing.addTraversal(clone.integrateChild(traversal.clone()));
        }
        return clone;
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getLocalChildren() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> treeTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(treeTraversal));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH, TraverserRequirement.SIDE_EFFECTS);
    }

    ////////////////

    public static final class TreeSideEffectMapReduce extends StaticMapReduce<Object, Tree, Object, Tree, Tree> {

        public static final String TREE_SIDE_EFFECT_STEP_SIDE_EFFECT_KEY = "gremlin.treeSideEffectStep.sideEffectKey";

        private String sideEffectKey;

        private TreeSideEffectMapReduce() {

        }

        public TreeSideEffectMapReduce(final TreeSideEffectStep step) {
            this.sideEffectKey = step.getSideEffectKey();
        }

        @Override
        public void storeState(final Configuration configuration) {
            super.storeState(configuration);
            configuration.setProperty(TREE_SIDE_EFFECT_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
        }

        @Override
        public void loadState(final Configuration configuration) {
            this.sideEffectKey = configuration.getString(TREE_SIDE_EFFECT_STEP_SIDE_EFFECT_KEY);
        }

        @Override
        public boolean doStage(final Stage stage) {
            return stage.equals(Stage.MAP);
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<Object, Tree> emitter) {
            VertexTraversalSideEffects.of(vertex).<Tree<?>>get(this.sideEffectKey).ifPresent(tree -> tree.splitParents().forEach(branches -> emitter.emit(branches.keySet().iterator().next(), branches)));
        }

        /*
        @Override
        public void combine(final NullObject key, final Iterator<Tree> values, final ReduceEmitter<NullObject, Tree> emitter) {
            this.reduce(key, values, emitter);
        }

        @Override
        public void reduce(final NullObject key, final Iterator<Tree> values, final ReduceEmitter<NullObject, Tree> emitter) {
            final Tree tree = new Tree();
            values.forEachRemaining(tree::addTree);
            emitter.emit(tree);
        }
        */

        @Override
        public Tree generateFinalResult(final Iterator<KeyValue<Object, Tree>> keyValues) {
            final Tree result = new Tree();
            keyValues.forEachRemaining(keyValue -> result.addTree(keyValue.getValue()));
            return result;
        }

        @Override
        public String getMemoryKey() {
            return this.sideEffectKey;
        }
    }

}
