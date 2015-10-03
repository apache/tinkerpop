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
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BarrierStep;
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
public final class GroupStep<S, K, V> extends ReducingBarrierStep<S, Map<K, V>> implements MapReducer, TraversalParent {

    private char state = 'k';

    private Traversal.Admin<S, K> keyTraversal = null;
    private Traversal.Admin<S, V> valueTraversal = this.integrateChild((Traversal.Admin) __.<V>fold().asAdmin());

    public GroupStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier((Supplier) new GroupMapSupplier());
        this.setBiFunction(new GroupBiFunction(this));
    }

    @Override
    public <A, B> List<Traversal.Admin<A, B>> getLocalChildren() {
        final List<Traversal.Admin<A, B>> children = new ArrayList<>(3);
        if (null != this.keyTraversal)
            children.add((Traversal.Admin) this.keyTraversal);
        if (null != this.valueTraversal)
            children.add((Traversal.Admin) this.valueTraversal);
        ;
        return children;
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> kvrTraversal) {
        if ('k' == this.state) {
            this.keyTraversal = this.integrateChild(kvrTraversal);
            this.state = 'v';
        } else if ('v' == this.state) {
            this.valueTraversal = this.integrateChild(kvrTraversal);
            this.state = 'x';
        } else {
            throw new IllegalStateException("The key and value traversals for group()-step have already been set: " + this);
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.SIDE_EFFECTS, TraverserRequirement.BULK);
    }

    @Override
    public GroupStep<S, K, V> clone() {
        final GroupStep<S, K, V> clone = (GroupStep<S, K, V>) super.clone();
        if (null != this.keyTraversal)
            clone.keyTraversal = clone.integrateChild(this.keyTraversal.clone());
        if (null != this.valueTraversal)
            clone.valueTraversal = clone.integrateChild(this.valueTraversal.clone());
        clone.setBiFunction(new GroupBiFunction<>((GroupStep) clone));
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        for (final Traversal.Admin traversal : this.getLocalChildren()) {
            result ^= traversal.hashCode();
        }
        return result;
    }

    @Override
    public MapReduce<K, V, K, V, Map<K, V>> getMapReduce() {
        return new GroupMapReduce(this);
    }

    @Override
    public Traverser<Map<K, V>> processNextStart() {
        if (this.byPass) {
            final Traverser.Admin<S> traverser = this.starts.next();
            final Object[] kvPair = new Object[]{TraversalUtil.applyNullable(traverser, (Traversal.Admin<S, Map>) this.keyTraversal), traverser.get()};
            return traverser.asAdmin().split(kvPair, (Step) this);
        } else {
            return super.processNextStart();
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.keyTraversal, this.valueTraversal);
    }

    ///////////

    private static class GroupBiFunction<S, K, V> implements BiFunction<Map<K, Traversal.Admin<S, V>>, Traverser.Admin<S>, Map<K, Traversal.Admin<S, V>>>, Serializable {

        private final GroupStep<S, K, V> groupStep;

        private GroupBiFunction(final GroupStep<S, K, V> groupStep) {
            this.groupStep = groupStep;
        }

        @Override
        public Map<K, Traversal.Admin<S, V>> apply(final Map<K, Traversal.Admin<S, V>> mutatingSeed, final Traverser.Admin<S> traverser) {
            final K key = TraversalUtil.applyNullable(traverser, this.groupStep.keyTraversal);
            Traversal.Admin<S, V> traversal = mutatingSeed.get(key);
            if (null == traversal) {
                traversal = this.groupStep.valueTraversal.clone();
                mutatingSeed.put(key, traversal);
            }
            traversal.addStart(traverser.split());
            TraversalHelper.getStepsOfClass(BarrierStep.class, traversal).stream().findFirst().ifPresent(BarrierStep::processAllStarts);
            return mutatingSeed;
        }
    }

    //////////

    private static class GroupMap<K, V, R> extends HashMap<K, Traversal.Admin<V, R>> implements FinalGet<Map<K, R>> {

        @Override
        public Map<K, R> getFinal() {
            final Map<K, R> reduceMap = new HashMap<>();
            this.forEach((key, traversal) -> reduceMap.put(key, traversal.next()));
            return reduceMap;
        }
    }

    private static class GroupMapSupplier implements Supplier<GroupMap>, Serializable {

        private GroupMapSupplier() {
        }

        @Override
        public GroupMap get() {
            return new GroupMap();
        }
    }

    ///////////

    public static final class GroupMapReduce<S, K, V> implements MapReduce<K, S, K, V, Map<K, V>> {

        public static final String GROUP_BY_STEP_STEP_ID = "gremlin.groupStep.stepId";

        private String groupStepId;
        private Traversal.Admin<S, V> valueTraversal;

        private GroupMapReduce() {

        }

        public GroupMapReduce(final GroupStep<S, K, V> step) {
            this.groupStepId = step.getId();
            this.valueTraversal = step.valueTraversal;
        }

        @Override
        public void storeState(final Configuration configuration) {
            MapReduce.super.storeState(configuration);
            configuration.setProperty(GROUP_BY_STEP_STEP_ID, this.groupStepId);
        }

        @Override
        public void loadState(final Graph graph, final Configuration configuration) {
            this.groupStepId = configuration.getString(GROUP_BY_STEP_STEP_ID);
            this.valueTraversal = ((GroupStep) new TraversalMatrix<>(TraversalVertexProgram.getTraversal(graph, configuration)).getStepById(this.groupStepId)).valueTraversal.clone();
        }

        @Override
        public boolean doStage(final Stage stage) {
            return !stage.equals(Stage.COMBINE);
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<K, S> emitter) {
            vertex.<TraverserSet<Object[]>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(traverser -> {
                final Object[] objects = traverser.get();
                emitter.emit((K) objects[0], (S) objects[1]);

            }));
        }

        @Override
        public void reduce(final K key, final Iterator<S> values, final ReduceEmitter<K, V> emitter) {
            Traversal.Admin<S, V> reduceTraversalClone = this.valueTraversal.clone();
            reduceTraversalClone.addStarts(reduceTraversalClone.getTraverserGenerator().generateIterator((Iterator)values, reduceTraversalClone.getStartStep(), 1l));
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
                if (null != clone.valueTraversal)
                    clone.valueTraversal = this.valueTraversal.clone();
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