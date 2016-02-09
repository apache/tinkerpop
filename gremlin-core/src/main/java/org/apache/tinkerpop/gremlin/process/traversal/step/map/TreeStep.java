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

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
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
public final class TreeStep<S> extends ReducingBarrierStep<S, Tree> implements MapReducer, TraversalParent, PathProcessor {

    private TraversalRing<Object, Object> traversalRing = new TraversalRing<>();

    public TreeStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier((Supplier) TreeSupplier.instance());
        this.setBiFunction(new TreeBiFunction(this));
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
        clone.traversalRing = this.traversalRing.clone();
        clone.getLocalChildren().forEach(clone::integrateChild);
        clone.setBiFunction(new TreeBiFunction<>(clone));
        return clone;
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

    ///////////

    private static class TreeBiFunction<S> implements BiFunction<Tree, Traverser<S>, Tree>, Serializable {

        private final TreeStep<S> treeStep;

        private TreeBiFunction(final TreeStep<S> treeStep) {
            this.treeStep = treeStep;
        }

        @Override
        public Tree apply(final Tree mutatingSeed, final Traverser<S> traverser) {
            Tree depth = mutatingSeed;
            final Path path = traverser.path();
            for (int i = 0; i < path.size(); i++) {
                final Object object = TraversalUtil.apply(path.<Object>get(i), this.treeStep.traversalRing.next());
                if (!depth.containsKey(object))
                    depth.put(object, new Tree<>());
                depth = (Tree) depth.get(object);
            }
            this.treeStep.traversalRing.reset();
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
            return keyValues.hasNext() ? keyValues.next().getValue() : new Tree();
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