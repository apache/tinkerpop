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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.Path;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.graph.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.TreeSupplier;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TreeStep<S> extends ReducingBarrierStep<S, Tree> implements MapReducer, TraversalParent {

    private TraversalRing<Object, Object> traversalRing = new TraversalRing<>();

    public TreeStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier((Supplier) TreeSupplier.instance());
        this.setBiFunction(new TreeBiFunction());
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

    @Override
    public MapReduce<MapReduce.NullObject, Tree, MapReduce.NullObject, Tree, Tree> getMapReduce() {
        return TreeMapReduce.instance();
    }

    @Override
    public TreeStep<S> clone() {
        final TreeStep<S> clone = (TreeStep<S>) super.clone();
        clone.traversalRing = new TraversalRing<>();
        for (final Traversal.Admin<Object, Object> traversal : this.traversalRing.getTraversals()) {
            clone.traversalRing.addTraversal(clone.integrateChild(traversal.clone()));
        }
        return clone;
    }

    @Override
    public Traverser<Tree> processNextStart() {
        if (this.byPass) {
            final Traverser.Admin<S> traverser = this.starts.next();
            return traverser.split(this.reducingBiFunction.apply(new Tree(), traverser), this);
        } else {
            return super.processNextStart();
        }
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.traversalRing);
    }

    ///////////

    private class TreeBiFunction implements BiFunction<Tree, Traverser<S>, Tree>, Serializable {

        private TreeBiFunction() {

        }

        @Override
        public Tree apply(final Tree mutatingSeed, final Traverser<S> traverser) {
            Tree depth = mutatingSeed;
            final Path path = traverser.path();
            for (int i = 0; i < path.size(); i++) {
                final Object object = TraversalUtil.apply(path.<Object>get(i), TreeStep.this.traversalRing.next());
                if (!depth.containsKey(object))
                    depth.put(object, new Tree<>());
                depth = (Tree) depth.get(object);
            }
            TreeStep.this.traversalRing.reset();
            return mutatingSeed;
        }
    }

    ///////////

    public static final class TreeMapReduce extends StaticMapReduce<MapReduce.NullObject, Tree, MapReduce.NullObject, Tree, Tree> {

        private static final TreeMapReduce INSTANCE = new TreeMapReduce();

        private TreeMapReduce() {

        }

        @Override
        public boolean doStage(final Stage stage) {
            return true;
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<NullObject, Tree> emitter) {
            vertex.<TraverserSet<Tree>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(traverser -> emitter.emit(traverser.get())));
        }

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

        @Override
        public Tree generateFinalResult(final Iterator<KeyValue<NullObject, Tree>> keyValues) {
            final Tree tree = new Tree();
            keyValues.forEachRemaining(keyValue -> tree.addTree(keyValue.getValue()));
            return tree;
        }

        @Override
        public String getMemoryKey() {
            return REDUCING;
        }

        public static final TreeMapReduce instance() {
            return INSTANCE;
        }
    }

}