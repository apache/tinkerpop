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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.EngineDependent;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GroupStepHelper;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupStep<S, K, V> extends ReducingBarrierStep<S, Map<K, V>> implements MapReducer, EngineDependent, TraversalParent {

    private char state = 'k';

    private Traversal.Admin<S, K> keyTraversal = null;
    private Traversal.Admin<S, ?> valueTraversal = this.integrateChild(__.identity().asAdmin());   // used in OLAP
    private Traversal.Admin<?, V> reduceTraversal = this.integrateChild(__.fold().asAdmin());      // used in OLAP
    private Traversal.Admin<S, V> valueReduceTraversal = this.integrateChild(__.fold().asAdmin()); // used in OLTP
    private boolean byPass = false;

    public GroupStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier((Supplier) new GroupStepHelper.GroupMapSupplier());
        this.setBiFunction(new GroupBiFunction(this));
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        this.byPass = traversalEngine.isComputer();
    }

    @Override
    public List<Traversal.Admin<?, ?>> getLocalChildren() {
        final List<Traversal.Admin<?, ?>> children = new ArrayList<>(4);
        if (null != this.keyTraversal)
            children.add((Traversal.Admin) this.keyTraversal);
        children.add(this.valueReduceTraversal);
        children.add(this.valueTraversal);
        children.add(this.reduceTraversal);
        return children;
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> kvTraversal) {
        if ('k' == this.state) {
            this.keyTraversal = this.integrateChild(kvTraversal);
            this.state = 'v';
        } else if ('v' == this.state) {
            this.valueReduceTraversal = this.integrateChild(GroupStepHelper.convertValueTraversal(kvTraversal));
            final List<Traversal.Admin<?, ?>> splitTraversal = GroupStepHelper.splitOnBarrierStep(this.valueReduceTraversal);
            this.valueTraversal = this.integrateChild(splitTraversal.get(0));
            this.reduceTraversal = this.integrateChild(splitTraversal.get(1));
            this.state = 'x';
        } else {
            throw new IllegalStateException("The key and value traversals for group()-step have already been set: " + this);
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT, TraverserRequirement.BULK);
    }

    @Override
    public GroupStep<S, K, V> clone() {
        final GroupStep<S, K, V> clone = (GroupStep<S, K, V>) super.clone();
        if (null != this.keyTraversal)
            clone.keyTraversal = clone.integrateChild(this.keyTraversal.clone());
        clone.valueReduceTraversal = clone.integrateChild(this.valueReduceTraversal.clone());
        clone.valueTraversal = clone.integrateChild(this.valueTraversal.clone());
        clone.reduceTraversal = clone.integrateChild(this.reduceTraversal.clone());
        clone.setBiFunction(new GroupBiFunction<>((GroupStep) clone));
        return clone;
    }

    @Override
    public int hashCode() {
        int result = this.valueReduceTraversal.hashCode();
        if (this.keyTraversal != null) result ^= this.keyTraversal.hashCode();
        return result;
    }

    @Override
    public MapReduce<K, Collection<?>, K, V, Map<K, V>> getMapReduce() {
        return new GroupMapReduce<>(this);
    }

    @Override
    public Traverser<Map<K, V>> processNextStart() {
        if (this.byPass) {
            final Traverser.Admin<S> traverser = this.starts.next();
            final K key = TraversalUtil.applyNullable(traverser, this.keyTraversal);
            this.valueTraversal.addStart(traverser);
            final BulkSet<?> value = this.valueTraversal.toBulkSet();
            return traverser.asAdmin().split(new Object[]{key, value}, (Step) this);
        } else {
            return super.processNextStart();
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.keyTraversal, this.valueReduceTraversal);
    }

    ///////////

    private static class GroupBiFunction<S, K, V> implements BiFunction<Map<K, Traversal.Admin<S, V>>, Traverser.Admin<S>, Map<K, Traversal.Admin<S, V>>>, Serializable {

        private final GroupStep<S, K, V> groupStep;
        private Map<K, Integer> counters = new HashMap<>();

        private GroupBiFunction(final GroupStep<S, K, V> groupStep) {
            this.groupStep = groupStep;
        }

        @Override
        public Map<K, Traversal.Admin<S, V>> apply(final Map<K, Traversal.Admin<S, V>> mutatingSeed, final Traverser.Admin<S> traverser) {
            final K key = TraversalUtil.applyNullable(traverser, this.groupStep.keyTraversal);
            Traversal.Admin<S, V> traversal = mutatingSeed.get(key);
            if (null == traversal) {
                traversal = this.groupStep.valueReduceTraversal.clone();
                this.counters.put(key, 0);
                mutatingSeed.put(key, traversal);
            }

            traversal.addStart(traverser);
            final int count = this.counters.compute(key, (k, i) -> ++i);
            if (count > 10000) {
                this.counters.put(key, 0);
                TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, traversal).ifPresent(Barrier::processAllStarts);
            }
            return mutatingSeed;
        }
    }

    ///////////

    public static final class GroupMapReduce<S, K, V> implements MapReduce<K, Collection<?>, K, V, Map<K, V>> {

        public static final String GROUP_BY_STEP_STEP_ID = "gremlin.groupStep.stepId";

        private String groupStepId;
        private Traversal.Admin<?, V> reduceTraversal;

        private GroupMapReduce() {

        }

        public GroupMapReduce(final GroupStep<S, K, V> step) {
            this.groupStepId = step.getId();
            this.reduceTraversal = step.reduceTraversal.clone();
        }

        @Override
        public void storeState(final Configuration configuration) {
            MapReduce.super.storeState(configuration);
            configuration.setProperty(GROUP_BY_STEP_STEP_ID, this.groupStepId);
        }

        @Override
        public void loadState(final Graph graph, final Configuration configuration) {
            this.groupStepId = configuration.getString(GROUP_BY_STEP_STEP_ID);
            this.reduceTraversal = ((GroupStep) new TraversalMatrix<>(TraversalVertexProgram.getTraversal(graph, configuration)).getStepById(this.groupStepId)).reduceTraversal.clone();
        }

        @Override
        public boolean doStage(final Stage stage) {
            return !stage.equals(Stage.COMBINE);
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<K, Collection<?>> emitter) {
            vertex.<TraverserSet<Object[]>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(traverser -> {
                final Object[] objects = traverser.get();
                emitter.emit((K) objects[0], (Collection<?>) objects[1]);
            }));
        }

        @Override
        public void reduce(final K key, final Iterator<Collection<?>> values, final ReduceEmitter<K, V> emitter) {
            Traversal.Admin<?, V> reduceTraversalClone = this.reduceTraversal.clone();
            while (values.hasNext()) {
                reduceTraversalClone.addStarts(reduceTraversalClone.getTraverserGenerator().generateIterator(values.next().iterator(), (Step) reduceTraversalClone.getStartStep(), 1l));
                TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, reduceTraversalClone).ifPresent(Barrier::processAllStarts);
            }
            emitter.emit(key, reduceTraversalClone.next());
        }

        @Override
        public Map<K, V> generateFinalResult(final Iterator<KeyValue<K, V>> keyValues) {
            final Map<K, V> map = new HashMap<>();
            keyValues.forEachRemaining(keyValue -> map.put(keyValue.getKey(), keyValue.getValue()));
            return map;
        }

        @Override
        public String getMemoryKey() {
            return REDUCING;
        }

        @Override
        public GroupMapReduce<S, K, V> clone() {
            try {
                final GroupMapReduce<S, K, V> clone = (GroupMapReduce<S, K, V>) super.clone();
                clone.reduceTraversal = this.reduceTraversal.clone();
                return clone;
            } catch (final CloneNotSupportedException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        @Override
        public String toString() {
            return StringFactory.mapReduceString(this, this.getMemoryKey());
        }
    }
}