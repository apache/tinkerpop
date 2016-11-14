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
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
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
    private Traversal.Admin<S, K> keyTraversal;
    private Traversal.Admin<S, ?> preTraversal;
    private Traversal.Admin<S, V> valueTraversal;

    public GroupStep(final Traversal.Admin traversal) {
        super(traversal);
        this.valueTraversal = this.integrateChild(__.fold().asAdmin());
        this.preTraversal = this.integrateChild(generatePreTraversal(this.valueTraversal));
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
            this.preTraversal = this.integrateChild(generatePreTraversal(this.valueTraversal));
            this.setReducingBiOperator(new GroupBiOperator<>(this.valueTraversal));
            this.state = 'x';
        } else {
            throw new IllegalStateException("The key and value traversals for group()-step have already been set: " + this);
        }
    }

    @Override
    public Map<K, V> projectTraverser(final Traverser.Admin<S> traverser) {
        final Map<K, V> map = new HashMap<>(1);
        if (null == this.preTraversal) {
            map.put(TraversalUtil.applyNullable(traverser, this.keyTraversal), (V) traverser);
        } else {
            final TraverserSet traverserSet = new TraverserSet<>();
            this.preTraversal.reset();
            this.preTraversal.addStart(traverser);
            while(this.preTraversal.hasNext()) {
                traverserSet.add(this.preTraversal.nextTraverser());
            }
            map.put(TraversalUtil.applyNullable(traverser, this.keyTraversal), (V) traverserSet);
        }
        return map;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.keyTraversal, this.valueTraversal);
    }

    @Override
    public List<Traversal.Admin<?, ?>> getLocalChildren() {
        final List<Traversal.Admin<?, ?>> children = new ArrayList<>(3);
        if (null != this.keyTraversal)
            children.add(this.keyTraversal);
        children.add(this.valueTraversal);
        if (null != this.preTraversal)
            children.add(this.preTraversal);
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
        clone.preTraversal = (Traversal.Admin<S, ?>) GroupStep.generatePreTraversal(clone.valueTraversal);
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

        // size limit before Barrier.processAllStarts() to lazy reduce
        private static final int SIZE_LIMIT = 1000;

        private Traversal.Admin<?, V> valueTraversal;
        private Barrier barrierStep;

        public GroupBiOperator(final Traversal.Admin<?, V> valueTraversal) {
            // if there is a lambda that can not be serialized, then simply use TraverserSets
            if (TraversalHelper.hasStepOfAssignableClassRecursively(LambdaHolder.class, valueTraversal)) {
                this.valueTraversal = null;
                this.barrierStep = null;
            } else {
                this.valueTraversal = valueTraversal;
                this.barrierStep = TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, this.valueTraversal).orElse(null);
            }
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
                    // TRAVERSER
                    if (objectA instanceof Traverser.Admin) {
                        if (objectB instanceof Traverser.Admin) {
                            final TraverserSet set = new TraverserSet();
                            set.add((Traverser.Admin) objectA);
                            set.add((Traverser.Admin) objectB);
                            objectA = set;
                        } else if (objectB instanceof TraverserSet) {
                            final TraverserSet set = (TraverserSet) objectB;
                            set.add((Traverser.Admin) objectA);
                            if (null != this.barrierStep && set.size() > SIZE_LIMIT) {
                                this.valueTraversal.reset();
                                ((Step) this.barrierStep).addStarts(set.iterator());
                                objectA = this.barrierStep.nextBarrier();
                            } else
                                objectA = objectB;
                        } else if (objectB instanceof Pair) {
                            final TraverserSet set = (TraverserSet) ((Pair) objectB).getValue0();
                            set.add((Traverser.Admin) objectA);
                            if (set.size() > SIZE_LIMIT) {    // barrier step can never be null -- no need to check
                                this.valueTraversal.reset();
                                ((Step) this.barrierStep).addStarts(set.iterator());
                                this.barrierStep.addBarrier(((Pair) objectB).getValue1());
                                objectA = this.barrierStep.nextBarrier();
                            } else
                                objectA = Pair.with(set, ((Pair) objectB).getValue1());
                        } else
                            objectA = Pair.with(new TraverserSet((Traverser.Admin) objectA), objectB);
                        // TRAVERSER SET
                    } else if (objectA instanceof TraverserSet) {
                        if (objectB instanceof Traverser.Admin) {
                            final TraverserSet set = (TraverserSet) objectA;
                            set.add((Traverser.Admin) objectB);
                            if (null != this.barrierStep && set.size() > SIZE_LIMIT) {
                                this.valueTraversal.reset();
                                ((Step) this.barrierStep).addStarts(set.iterator());
                                objectA = this.barrierStep.nextBarrier();
                            }
                        } else if (objectB instanceof TraverserSet) {
                            final TraverserSet set = (TraverserSet) objectA;
                            set.addAll((TraverserSet) objectB);
                            if (null != this.barrierStep && set.size() > SIZE_LIMIT) {
                                this.valueTraversal.reset();
                                ((Step) this.barrierStep).addStarts(set.iterator());
                                objectA = this.barrierStep.nextBarrier();
                            }
                        } else if (objectB instanceof Pair) {
                            final TraverserSet set = (TraverserSet) objectA;
                            set.addAll((TraverserSet) ((Pair) objectB).getValue0());
                            if (set.size() > SIZE_LIMIT) {  // barrier step can never be null -- no need to check
                                this.valueTraversal.reset();
                                ((Step) this.barrierStep).addStarts(set.iterator());
                                this.barrierStep.addBarrier(((Pair) objectB).getValue1());
                                objectA = this.barrierStep.nextBarrier();
                            } else
                                objectA = Pair.with(set, ((Pair) objectB).getValue1());
                        } else
                            objectA = Pair.with(objectA, objectB);
                        // TRAVERSER SET + BARRIER
                    } else if (objectA instanceof Pair) {
                        if (objectB instanceof Traverser.Admin) {
                            final TraverserSet set = ((TraverserSet) ((Pair) objectA).getValue0());
                            set.add((Traverser.Admin) objectB);
                            if (set.size() > SIZE_LIMIT) { // barrier step can never be null -- no need to check
                                this.valueTraversal.reset();
                                ((Step) this.barrierStep).addStarts(set.iterator());
                                this.barrierStep.addBarrier(((Pair) objectA).getValue1());
                                objectA = this.barrierStep.nextBarrier();
                            }
                        } else if (objectB instanceof TraverserSet) {
                            final TraverserSet set = (TraverserSet) ((Pair) objectA).getValue0();
                            set.addAll((TraverserSet) objectB);
                            if (set.size() > SIZE_LIMIT) {   // barrier step can never be null -- no need to check
                                this.valueTraversal.reset();
                                ((Step) this.barrierStep).addStarts(set.iterator());
                                this.barrierStep.addBarrier(((Pair) objectA).getValue1());
                                objectA = this.barrierStep.nextBarrier();
                            }
                        } else if (objectB instanceof Pair) {
                            this.valueTraversal.reset();
                            this.barrierStep.addBarrier(((Pair) objectA).getValue1());
                            this.barrierStep.addBarrier(((Pair) objectB).getValue1());
                            ((Step) this.barrierStep).addStarts(((TraverserSet) ((Pair) objectA).getValue0()).iterator());
                            ((Step) this.barrierStep).addStarts(((TraverserSet) ((Pair) objectB).getValue0()).iterator());
                            objectA = this.barrierStep.nextBarrier();
                        } else {
                            this.valueTraversal.reset();
                            this.barrierStep.addBarrier(((Pair) objectA).getValue1());
                            this.barrierStep.addBarrier(objectB);
                            ((Step) this.barrierStep).addStarts(((TraverserSet) ((Pair) objectA).getValue0()).iterator());
                            objectA = this.barrierStep.nextBarrier();
                        }
                        // BARRIER
                    } else {
                        if (objectB instanceof Traverser.Admin) {
                            objectA = Pair.with(new TraverserSet<>((Traverser.Admin) objectB), objectA);
                        } else if (objectB instanceof TraverserSet) {
                            objectA = Pair.with(objectB, objectA);
                        } else if (objectB instanceof Pair) {
                            this.valueTraversal.reset();
                            this.barrierStep.addBarrier(objectA);
                            this.barrierStep.addBarrier(((Pair) objectB).getValue1());
                            ((Step) this.barrierStep).addStarts(((TraverserSet) ((Pair) objectB).getValue0()).iterator());
                            objectA = this.barrierStep.nextBarrier();
                        } else {
                            this.valueTraversal.reset();
                            this.barrierStep.addBarrier(objectA);
                            this.barrierStep.addBarrier(objectB);
                            objectA = this.barrierStep.nextBarrier();
                        }
                    }
                }
                mapA.put(key, (V) objectA);
            }
            return mapA;
        }

        // necessary to control Java Serialization to ensure proper clearing of internal traverser data
        private void writeObject(final ObjectOutputStream outputStream) throws IOException {
            // necessary as a non-root child is being sent over the wire
            if (null != this.valueTraversal) this.valueTraversal.setParent(EmptyStep.instance());
            outputStream.writeObject(null == this.valueTraversal ? null : this.valueTraversal.clone()); // todo: reset() instead?
        }

        private void readObject(final ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
            this.valueTraversal = (Traversal.Admin<?, V>) inputStream.readObject();
            this.barrierStep = null == this.valueTraversal ? null : TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, this.valueTraversal).orElse(null);
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

    public static Traversal.Admin<?, ?> generatePreTraversal(final Traversal.Admin<?, ?> valueTraversal) {
        if (!TraversalHelper.hasStepOfAssignableClass(Barrier.class, valueTraversal))
            return valueTraversal.clone();
        final Traversal.Admin<?, ?> first = __.identity().asAdmin();
        boolean updated = false;
        for (final Step step : valueTraversal.getSteps()) {
            if (step instanceof Barrier)
                break;
            first.addStep(step.clone());
            updated = true;
        }
        return updated ? first : null;
    }

    public static <K, V> Map<K, V> doFinalReduction(final Map<K, Object> map, final Traversal.Admin<?, V> valueTraversal) {
        final Map<K, V> reducedMap = new HashMap<>(map.size());
        final Barrier reducingBarrierStep = TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, valueTraversal).orElse(null);
        IteratorUtils.removeOnNext(map.entrySet().iterator()).forEachRemaining(entry -> {
            if (null == reducingBarrierStep) {
                if (entry.getValue() instanceof TraverserSet) {
                    if (!((TraverserSet) entry.getValue()).isEmpty())
                        reducedMap.put(entry.getKey(), ((TraverserSet<V>) entry.getValue()).peek().get());
                } else
                    reducedMap.put(entry.getKey(), (V) entry.getValue());
            } else {
                valueTraversal.reset();
                if (entry.getValue() instanceof Traverser.Admin)
                    ((Step) reducingBarrierStep).addStart((Traverser.Admin) entry.getValue());
                else if (entry.getValue() instanceof TraverserSet)
                    ((Step) reducingBarrierStep).addStarts(((TraverserSet) entry.getValue()).iterator());
                else if (entry.getValue() instanceof Pair) {
                    ((Step) reducingBarrierStep).addStarts(((TraverserSet) (((Pair) entry.getValue()).getValue0())).iterator());
                    reducingBarrierStep.addBarrier((((Pair) entry.getValue()).getValue1()));
                } else
                    reducingBarrierStep.addBarrier(entry.getValue());
                if (valueTraversal.hasNext())
                    reducedMap.put(entry.getKey(), valueTraversal.next());
            }
        });
        assert map.isEmpty();
        map.clear();
        map.putAll(reducedMap);
        return (Map<K, V>) map;
    }
}


