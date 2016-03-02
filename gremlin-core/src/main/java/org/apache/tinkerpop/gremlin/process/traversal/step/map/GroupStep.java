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
public final class GroupStep<S, K, V> extends ReducingBarrierStep<S, Map<K, V>> implements ByModulating, TraversalParent, GraphComputing {

    private char state = 'k';
    private Traversal.Admin<S, K> keyTraversal = null;
    private Traversal.Admin<S, ?> valueTraversal = this.integrateChild(__.identity().asAdmin());   // used in OLAP
    private Traversal.Admin<?, V> reduceTraversal = this.integrateChild(__.fold().asAdmin());      // used in OLAP
    public Traversal.Admin<S, V> valueReduceTraversal = this.integrateChild(__.fold().asAdmin()); // used in OLTP
    private boolean onGraphComputer = false;


    public GroupStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setReducingBiOperator(new GroupBiOperator<>(this.valueReduceTraversal));
        this.setSeedSupplier(HashMapSupplier.instance());
    }

    @Override
    public void onGraphComputer() {
        this.onGraphComputer = true;
        this.setReducingBiOperator(GroupBiOperator.computerInstance());
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> kvTraversal) {
        if ('k' == this.state) {
            this.keyTraversal = this.integrateChild(kvTraversal);
            this.state = 'v';
        } else if ('v' == this.state) {
            this.valueReduceTraversal = this.integrateChild(convertValueTraversal(kvTraversal));
            final List<Traversal.Admin<?, ?>> splitTraversal = splitOnBarrierStep(this.valueReduceTraversal);
            this.valueTraversal = this.integrateChild(splitTraversal.get(0));
            this.reduceTraversal = this.integrateChild(splitTraversal.get(1));
            this.state = 'x';
            this.setReducingBiOperator(new GroupBiOperator<>(this.valueReduceTraversal));
        } else {
            throw new IllegalStateException("The key and value traversals for group()-step have already been set: " + this);
        }
    }

    @Override
    public Map<K, V> projectTraverser(final Traverser.Admin<S> traverser) {
        final Map<K, V> map = new HashMap<>();
        final K key = TraversalUtil.applyNullable(traverser, this.keyTraversal);
        if (this.onGraphComputer) {
            final TraverserSet traverserSet = new TraverserSet();
            this.valueTraversal.reset();
            this.valueTraversal.addStart(traverser);
            this.valueTraversal.getEndStep().forEachRemaining(t -> traverserSet.add(t.asAdmin()));
            map.put(key, (V) traverserSet);
        } else
            map.put(key, (V) traverser);
        return map;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.keyTraversal, this.valueReduceTraversal);
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
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT, TraverserRequirement.BULK, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public GroupStep<S, K, V> clone() {
        final GroupStep<S, K, V> clone = (GroupStep<S, K, V>) super.clone();
        if (null != this.keyTraversal)
            clone.keyTraversal = this.keyTraversal.clone();
        clone.valueReduceTraversal = this.valueReduceTraversal.clone();
        clone.valueTraversal = this.valueTraversal.clone();
        clone.reduceTraversal = this.reduceTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        integrateChild(this.keyTraversal);
        integrateChild(this.valueReduceTraversal);
        integrateChild(this.valueTraversal);
        integrateChild(this.reduceTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (this.keyTraversal != null) result ^= this.keyTraversal.hashCode();
        result ^= this.valueReduceTraversal.hashCode();
        return result;
    }

    @Override
    public Map<K, V> generateFinalResult(final Map<K, V> object) {
        return GroupStep.doFinalReduction((Map<K, Object>) object, this.onGraphComputer ? this.reduceTraversal : null);
    }

    ///////////////////////

    public static final class GroupBiOperator<K, V> implements BinaryOperator<Map<K, V>>, Serializable {

        private static final GroupBiOperator COMPUTER_INSTANCE = new GroupBiOperator();

        private transient Traversal.Admin<?, V> valueReduceTraversal;
        private transient Map<K, Integer> counters;
        private final boolean onGraphComputer;

        public GroupBiOperator(final Traversal.Admin<?, V> valueReduceTraversal) {
            this.valueReduceTraversal = valueReduceTraversal;
            this.counters = new HashMap<>();
            this.onGraphComputer = false;
        }

        public GroupBiOperator() {
            this.onGraphComputer = true;
        }

        @Override
        public Map<K, V> apply(final Map<K, V> mutatingSeed, final Map<K, V> map) {
            for (final K key : map.keySet()) {
                if (this.onGraphComputer) {
                    TraverserSet<?> traverserSet = (TraverserSet) mutatingSeed.get(key);
                    if (null == traverserSet) {
                        traverserSet = new TraverserSet<>();
                        mutatingSeed.put(key, (V) traverserSet);
                    }
                    traverserSet.addAll((TraverserSet) map.get(key));
                } else {
                    final Traverser.Admin traverser = (Traverser.Admin) map.get(key);
                    Traversal.Admin valueReduceTraversal = (Traversal.Admin) mutatingSeed.get(key);
                    if (null == valueReduceTraversal) {
                        this.counters.put(key, 0);
                        valueReduceTraversal = this.valueReduceTraversal.clone();
                        mutatingSeed.put(key, (V) valueReduceTraversal);
                    }
                    valueReduceTraversal.addStart(traverser);
                    final int count = this.counters.compute(key, (k, i) -> ++i);
                    if (count > 10000) {
                        this.counters.put(key, 0);
                        TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, valueReduceTraversal).ifPresent(Barrier::processAllStarts);
                    }
                }
            }
            return mutatingSeed;
        }

        public static final <K, V> GroupBiOperator<K, V> computerInstance() {
            return COMPUTER_INSTANCE;
        }
    }

    ///////////////////////

    public static <S, E> Traversal.Admin<S, E> convertValueTraversal(final Traversal.Admin<S, E> valueReduceTraversal) {
        if (valueReduceTraversal instanceof ElementValueTraversal ||
                valueReduceTraversal instanceof TokenTraversal ||
                valueReduceTraversal instanceof IdentityTraversal ||
                valueReduceTraversal.getStartStep() instanceof LambdaMapStep && ((LambdaMapStep) valueReduceTraversal.getStartStep()).getMapFunction() instanceof FunctionTraverser) {
            return (Traversal.Admin<S, E>) __.map(valueReduceTraversal).fold();
        } else {
            return valueReduceTraversal;
        }
    }

    public static List<Traversal.Admin<?, ?>> splitOnBarrierStep(final Traversal.Admin<?, ?> valueReduceTraversal) {
        if (TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, valueReduceTraversal).isPresent()) {
            final Traversal.Admin<?, ?> first = __.identity().asAdmin();
            final Traversal.Admin<?, ?> second = __.identity().asAdmin();
            boolean onSecond = false;
            for (final Step step : valueReduceTraversal.getSteps()) {
                if (step instanceof Barrier)
                    onSecond = true;
                if (onSecond)
                    second.addStep(step.clone());
                else
                    first.addStep(step.clone());
            }
            return Arrays.asList(first, second);
        } else {
            return Arrays.asList(valueReduceTraversal.clone(), __.identity().asAdmin());
        }
    }

    public static <K, V> Map<K, V> doFinalReduction(final Map<K, Object> map, final Traversal.Admin<?, V> reduceTraversal) {
        final Map<K, V> reducedMap = new HashMap<>();
        if (null != reduceTraversal) {
            for (final K key : map.keySet()) {
                final Traversal.Admin<?, V> reduceClone = reduceTraversal.clone();
                reduceClone.addStarts(((TraverserSet) map.get(key)).iterator());
                reducedMap.put(key, reduceClone.next());
            }
        } else
            map.forEach((key, traversal) -> reducedMap.put(key, ((Traversal.Admin<?, V>) traversal).next()));
        return reducedMap;
    }
}
