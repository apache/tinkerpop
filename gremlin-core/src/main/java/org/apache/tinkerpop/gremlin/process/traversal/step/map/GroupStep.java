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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.FunctionTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupStep<S, K, V> extends ReducingBarrierStep<S, Map<K, V>> implements ByModulating, TraversalParent, GraphComputing {

    private char state = 'k';
    private Traversal.Admin<S, K> keyTraversal = null;
    private Traversal.Admin<S, V> valueTraversal = this.integrateChild(__.fold().asAdmin());
    private Traversal.Admin<S, ?> preTraversal = null;   // used in OLAP
    private ReducingBarrierStep reducingBarrierStep = null; // used in OLAP
    private boolean onGraphComputer = false;

    public GroupStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setReducingBiOperator(new GroupBiOperator<>(this.valueTraversal, this.onGraphComputer));
        this.setSeedSupplier(HashMapSupplier.instance());
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> kvTraversal) {
        if ('k' == this.state) {
            this.keyTraversal = this.integrateChild(kvTraversal);
            this.state = 'v';
        } else if ('v' == this.state) {
            this.valueTraversal = this.integrateChild(convertValueTraversal(kvTraversal));
            this.setReducingBiOperator(new GroupBiOperator<>(this.valueTraversal, this.onGraphComputer));
            this.state = 'x';
        } else {
            throw new IllegalStateException("The key and value traversals for group()-step have already been set: " + this);
        }
    }

    @Override
    public Map<K, V> projectTraverser(final Traverser.Admin<S> traverser) {
        final Map<K, V> map = new HashMap<>(1);
        final K key = TraversalUtil.applyNullable(traverser, this.keyTraversal);
        if (this.onGraphComputer) {
            if (null == this.reducingBarrierStep) {
                final TraverserSet traverserSet = new TraverserSet();
                this.preTraversal.reset();
                this.preTraversal.addStart(traverser.split());
                this.preTraversal.getEndStep().forEachRemaining(traverserSet::add);
                map.put(key, (V) traverserSet);
            } else {
                this.valueTraversal.reset();
                this.valueTraversal.addStart(traverser.split());
                map.put(key, (V) this.reducingBarrierStep.nextBarrier());
            }
        } else
            map.put(key, (V) traverser);
        return map;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.keyTraversal, this.valueTraversal);
    }

    @Override
    public List<Traversal.Admin<?, ?>> getLocalChildren() {
        final List<Traversal.Admin<?, ?>> children = new ArrayList<>(4);
        if (null != this.keyTraversal)
            children.add((Traversal.Admin) this.keyTraversal);
        children.add(this.valueTraversal);
        return children;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT, TraverserRequirement.BULK);
    }

    @Override
    public GroupStep<S, K, V> clone() {
        final GroupStep<S, K, V> clone = (GroupStep<S, K, V>) super.clone();
        if (null != this.keyTraversal)
            clone.keyTraversal = this.keyTraversal.clone();
        clone.valueTraversal = this.valueTraversal.clone();
        if (null != this.preTraversal)
            clone.preTraversal = this.preTraversal.clone();
        final Optional<Barrier> optional = TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, clone.valueTraversal);
        if (optional.isPresent() && optional.get() instanceof ReducingBarrierStep)
            clone.reducingBarrierStep = (ReducingBarrierStep) optional.get();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        integrateChild(this.keyTraversal);
        integrateChild(this.valueTraversal);
        integrateChild(this.preTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (this.keyTraversal != null) result ^= this.keyTraversal.hashCode();
        result ^= this.valueTraversal.hashCode();
        return result;
    }

    @Override
    public Map<K, V> generateFinalResult(final Map<K, V> object) {
        return GroupStep.doFinalReduction((Map<K, Object>) object, this.valueTraversal, this.onGraphComputer);
    }

    @Override
    public void onGraphComputer() {
        this.preTraversal = this.integrateChild(splitOnBarrierStep(this.valueTraversal).get(0));
        final Optional<Barrier> optional = TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, this.valueTraversal);
        if (optional.isPresent() && optional.get() instanceof ReducingBarrierStep)
            this.reducingBarrierStep = (ReducingBarrierStep) optional.get();
        this.setReducingBiOperator(new GroupBiOperator<>(this.valueTraversal, this.onGraphComputer = true));
    }

    ///////////////////////

    public static final class GroupBiOperator<K, V> implements BinaryOperator<Map<K, V>>, Serializable {

        private Traversal.Admin<?, V> valueTraversal;
        private boolean onGraphComputer;
        private boolean hasReducingBarrier;
        private Map<K, Integer> counters;

        public GroupBiOperator(final Traversal.Admin<?, V> valueTraversal, final boolean onGraphComputer) {
            this.valueTraversal = valueTraversal;
            this.onGraphComputer = onGraphComputer;
            if (this.onGraphComputer) {
                final Optional<Barrier> optional = TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, this.valueTraversal);
                this.hasReducingBarrier = optional.isPresent() && optional.get() instanceof ReducingBarrierStep;
            } else {
                this.counters = new HashMap<>();
                this.hasReducingBarrier = false; // doesn't matter, its OLTP (no need to analyze the traversal)
            }
        }

        public GroupBiOperator() {
            // no-arg constructor for serialization
        }

        @Override
        public Map<K, V> apply(final Map<K, V> mapA, final Map<K, V> mapB) {
            for (final K key : mapB.keySet()) {
                if (this.onGraphComputer) {
                    final Object objectB = mapB.get(key);
                    // get mapA ready to receive data
                    if (this.hasReducingBarrier) {
                        // if there is a mid-barrier, process using midway reductions (reduces memory footprint)
                        final Traversal.Admin valueTraversalClone = this.valueTraversal.clone(); // be nice to just reset() but TinkerGraphComputer has thread issues
                        final ReducingBarrierStep reducingBarrierStep = TraversalHelper.getFirstStepOfAssignableClass(ReducingBarrierStep.class, valueTraversalClone).get();
                        final Object objectA = mapA.get(key);
                        if (null != objectA)
                            reducingBarrierStep.addBarrier(objectA);
                        reducingBarrierStep.addBarrier(objectB);
                        // only store the reduction barrier (i.e. seed), not the traversal
                        if (reducingBarrierStep.hasNextBarrier())
                            mapA.put(key, (V) reducingBarrierStep.nextBarrier());
                    } else {
                        // if there is no mid-traversal reducer, aggregate pre-barrier traversers into a traverser set (expensive, but that's that)
                        final Object objectA = mapA.get(key);
                        final TraverserSet traverserSet;
                        if (null == objectA) {
                            traverserSet = new TraverserSet();
                            mapA.put(key, (V) traverserSet);
                        } else
                            traverserSet = (TraverserSet) objectA;
                        traverserSet.addAll((TraverserSet) objectB);
                    }
                } else {
                    // for OLTP, do mid-barrier reductions if they exist, else don't.
                    // bulking is also available here because of addStart() prior to barrier
                    final Traverser.Admin traverser = (Traverser.Admin) mapB.get(key);
                    Traversal.Admin valueTraversalClone = (Traversal.Admin) mapA.get(key);
                    if (null == valueTraversalClone) {
                        this.counters.put(key, 0);
                        valueTraversalClone = this.valueTraversal.clone();
                        mapA.put(key, (V) valueTraversalClone);
                    }
                    valueTraversalClone.addStart(traverser);
                    if (this.counters.compute(key, (k, i) -> ++i) > 1000) {
                        this.counters.put(key, 0);
                        TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, valueTraversalClone).ifPresent(Barrier::processAllStarts);
                    }
                }
            }
            return mapA;
        }
    }

    ///////////////////////

    public static <S, E> Traversal.Admin<S, E> convertValueTraversal(final Traversal.Admin<S, E> valueTraversal) {
        if (valueTraversal instanceof ElementValueTraversal ||
                valueTraversal instanceof TokenTraversal ||
                valueTraversal instanceof IdentityTraversal ||
                valueTraversal.getStartStep() instanceof LambdaMapStep && ((LambdaMapStep) valueTraversal.getStartStep()).getMapFunction() instanceof FunctionTraverser) {
            return (Traversal.Admin<S, E>) __.map(valueTraversal).fold();
        } else {
            return valueTraversal;
        }
    }

    public static List<Traversal.Admin<?, ?>> splitOnBarrierStep(final Traversal.Admin<?, ?> valueTraversal) {
        if (TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, valueTraversal).isPresent()) {
            final Traversal.Admin<?, ?> first = __.identity().asAdmin();
            final Traversal.Admin<?, ?> second = __.identity().asAdmin();
            boolean onSecond = false;
            for (final Step step : valueTraversal.getSteps()) {
                if (step instanceof Barrier)
                    onSecond = true;
                if (onSecond)
                    second.addStep(step.clone());
                else
                    first.addStep(step.clone());
            }
            return Arrays.asList(first, second);
        } else {
            return Arrays.asList(valueTraversal.clone(), __.identity().asAdmin());
        }
    }

    public static <K, V> Map<K, V> doFinalReduction(final Map<K, Object> map, final Traversal.Admin<?, V> valueTraversal, final boolean onGraphComputer) {
        final Map<K, V> reducedMap = new HashMap<>(map.size());
        // if not on OLAP, who cares --- don't waste time computing barriers
        final Traversal.Admin<?, ?> postTraversal = onGraphComputer ? splitOnBarrierStep(valueTraversal.clone()).get(1) : null;
        final boolean hasReducingBarrier = onGraphComputer &&
                TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, valueTraversal).isPresent() &&
                TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, valueTraversal).get() instanceof ReducingBarrierStep;
        IteratorUtils.removeOnNext(map.entrySet().iterator()).forEachRemaining(entry -> {
            if (onGraphComputer) {
                if (hasReducingBarrier) {  // OLAP with reduction (barrier)
                    final Traversal.Admin<?, V> valueTraversalClone = valueTraversal.clone();
                    TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, valueTraversalClone).get().addBarrier(entry.getValue());
                    reducedMap.put(entry.getKey(), valueTraversalClone.next());
                } else {  // OLAP without reduction (traverser set)
                    postTraversal.reset();
                    postTraversal.addStarts(((TraverserSet) entry.getValue()).iterator());
                    reducedMap.put(entry.getKey(), (V) postTraversal.next());
                }
            } else   // OLTP is just a traversal
                reducedMap.put(entry.getKey(), ((Traversal.Admin<?, V>) entry.getValue()).next());
        });
        assert map.isEmpty();
        map.clear();
        map.putAll(reducedMap);
        return (Map<K, V>) map;
    }
}
