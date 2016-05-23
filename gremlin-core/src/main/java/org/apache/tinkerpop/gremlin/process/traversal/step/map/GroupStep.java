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
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupStep<S, K, V> extends ReducingBarrierStep<S, Map<K, V>> implements ByModulating, TraversalParent {

    private char state = 'k';
    private Traversal.Admin<S, K> keyTraversal = null;
    private Traversal.Admin<S, ?> preTraversal;
    private Traversal.Admin<S, V> valueTraversal;

    public GroupStep(final Traversal.Admin traversal) {
        super(traversal);
        this.valueTraversal = this.integrateChild(__.fold().asAdmin());
        this.preTraversal = this.integrateChild(splitOnBarrierStep(this.valueTraversal).get(0));
        this.setReducingBiOperator(new GroupBiOperator<>(this.valueTraversal));
        this.setSeedSupplier(HashMapSupplier.instance());
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> kvTraversal) {
        if ('k' == this.state) {
            this.keyTraversal = this.integrateChild(kvTraversal);
            this.state = 'v';
        } else if ('v' == this.state) {
            this.valueTraversal = this.integrateChild(convertValueTraversal(kvTraversal));
            this.preTraversal = this.integrateChild(splitOnBarrierStep(this.valueTraversal).get(0));
            this.setReducingBiOperator(new GroupBiOperator<>(this.valueTraversal));
            this.state = 'x';
        } else {
            throw new IllegalStateException("The key and value traversals for group()-step have already been set: " + this);
        }
    }

    @Override
    public Map<K, V> projectTraverser(final Traverser.Admin<S> traverser) {
        final Map<K, V> map = new HashMap<>(1);
        final TraverserSet traverserSet = new TraverserSet<>();
        this.preTraversal.reset();
        this.preTraversal.addStart(traverser.split());
        this.preTraversal.getEndStep().forEachRemaining(traverserSet::add);
        map.put(TraversalUtil.applyNullable(traverser, this.keyTraversal), (V) traverserSet);
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
        clone.preTraversal = this.integrateChild(GroupStep.splitOnBarrierStep(clone.valueTraversal).get(0));
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
        return GroupStep.doFinalReduction((Map<K, Object>) object, this.valueTraversal);
    }

    ///////////////////////

    public static final class GroupBiOperator<K, V> implements BinaryOperator<Map<K, V>>, Serializable {

        private Traversal.Admin<?, V> valueTraversal;
        private ReducingBarrierStep reducingBarrierStep = null;

        public GroupBiOperator(final Traversal.Admin<?, V> valueTraversal) {
            this.valueTraversal = valueTraversal.clone();
            this.reducingBarrierStep = TraversalHelper.getFirstStepOfAssignableClass(ReducingBarrierStep.class, this.valueTraversal).orElse(null);
        }

        public GroupBiOperator() {
            // no-arg constructor for serialization
        }

        @Override
        public Map<K, V> apply(final Map<K, V> mapA, final Map<K, V> mapB) {
            for (final K key : mapB.keySet()) {
                Object objectA = mapA.get(key);
                final Object objectB = mapB.get(key);
                assert null != objectB;
                if (null == objectA) {
                    objectA = objectB;
                } else {
                    if (objectA instanceof TraverserSet) {
                        if (objectB instanceof TraverserSet) {
                            final TraverserSet set = (TraverserSet) objectA;
                            set.addAll((TraverserSet) objectB);
                            if (null != this.reducingBarrierStep && set.size() > 1000) {
                                this.valueTraversal.reset();
                                this.reducingBarrierStep.addStarts(set.iterator());
                                objectA = this.reducingBarrierStep.nextBarrier();
                            }
                        } else if (objectB instanceof Pair) {
                            final TraverserSet set = (TraverserSet) objectA;
                            set.addAll((TraverserSet) ((Pair) objectB).getValue0());
                            if (set.size() > 1000) {
                                this.valueTraversal.reset();
                                this.reducingBarrierStep.addStarts(set.iterator());
                                this.reducingBarrierStep.addBarrier(((Pair) objectB).getValue1());
                                objectA = this.reducingBarrierStep.nextBarrier();
                            } else {
                                objectA = Pair.with(set, ((Pair) objectB).getValue1());
                            }
                        } else {
                            objectA = Pair.with(objectA, objectB);
                        }
                    } else if (objectA instanceof Pair) {
                        if (objectB instanceof TraverserSet) {
                            final TraverserSet set = (TraverserSet) ((Pair) objectA).getValue0();
                            set.addAll((TraverserSet) objectB);
                            if (null != this.reducingBarrierStep &&set.size() > 1000) {
                                this.valueTraversal.reset();
                                this.reducingBarrierStep.addStarts(set.iterator());
                                this.reducingBarrierStep.addBarrier(((Pair) objectA).getValue1());
                                objectA = this.reducingBarrierStep.nextBarrier();
                            }
                        } else if (objectB instanceof Pair) {
                            this.valueTraversal.reset();
                            this.reducingBarrierStep.addBarrier(((Pair) objectA).getValue1());
                            this.reducingBarrierStep.addBarrier(((Pair) objectB).getValue1());
                            this.reducingBarrierStep.addStarts(((TraverserSet) ((Pair) objectA).getValue0()).iterator());
                            this.reducingBarrierStep.addStarts(((TraverserSet) ((Pair) objectB).getValue0()).iterator());
                            objectA = this.reducingBarrierStep.nextBarrier();
                        } else {
                            this.valueTraversal.reset();
                            this.reducingBarrierStep.addBarrier(((Pair) objectA).getValue1());
                            this.reducingBarrierStep.addBarrier(objectB);
                            this.reducingBarrierStep.addStarts(((TraverserSet) ((Pair) objectA).getValue0()).iterator());
                            objectA = this.reducingBarrierStep.nextBarrier();
                        }
                    } else {
                        if (objectB instanceof TraverserSet) {
                            objectA = Pair.with(objectB, objectA);
                        } else if (objectB instanceof Pair) {
                            this.valueTraversal.reset();
                            this.reducingBarrierStep.addBarrier(objectA);
                            this.reducingBarrierStep.addBarrier(((Pair) objectB).getValue1());
                            this.reducingBarrierStep.addStarts(((TraverserSet) ((Pair) objectB).getValue0()).iterator());
                            objectA = this.reducingBarrierStep.nextBarrier();
                        } else {
                            this.valueTraversal.reset();
                            this.reducingBarrierStep.addBarrier(objectA);
                            this.reducingBarrierStep.addBarrier(objectB);
                            objectA = this.reducingBarrierStep.nextBarrier();
                        }
                    }
                }
                mapA.put(key, (V) objectA);
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

    public static <K, V> Map<K, V> doFinalReduction(final Map<K, Object> map, final Traversal.Admin<?, V> valueTraversal) {
        final Map<K, V> reducedMap = new HashMap<>(map.size());
        final ReducingBarrierStep reducingBarrierStep = TraversalHelper.getFirstStepOfAssignableClass(ReducingBarrierStep.class, valueTraversal).orElse(null);
        IteratorUtils.removeOnNext(map.entrySet().iterator()).forEachRemaining(entry -> {
            valueTraversal.reset();
            if (null == reducingBarrierStep) {
                reducedMap.put(entry.getKey(), entry.getValue() instanceof TraverserSet ?
                        ((TraverserSet<V>) entry.getValue()).iterator().next().get() :
                        (V) entry.getValue());
            } else {
                if (entry.getValue() instanceof TraverserSet)
                    reducingBarrierStep.addStarts(((TraverserSet) entry.getValue()).iterator());
                else if (entry.getValue() instanceof Pair) {
                    reducingBarrierStep.addStarts(((TraverserSet) (((Pair) entry.getValue()).getValue0())).iterator());
                    reducingBarrierStep.addBarrier((((Pair) entry.getValue()).getValue1()));
                } else
                    reducingBarrierStep.addBarrier(entry.getValue());
                reducedMap.put(entry.getKey(), valueTraversal.next());
            }
        });
        assert map.isEmpty();
        map.clear();
        map.putAll(reducedMap);
        return (Map<K, V>) map;
    }
}


