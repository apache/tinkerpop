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

import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaMapStep;
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
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupStep<S, K, V> extends SideEffectStep<S> implements SideEffectCapable, TraversalParent, ByModulating, GraphComputing {

    private char state = 'k';
    private Traversal.Admin<S, K> keyTraversal = null;
    private Traversal.Admin<S, ?> valueTraversal = this.integrateChild(__.identity().asAdmin());   // used in OLAP
    private Traversal.Admin<?, V> reduceTraversal = this.integrateChild(__.fold().asAdmin());      // used in OLAP
    private Traversal.Admin<S, V> valueReduceTraversal = this.integrateChild(__.fold().asAdmin()); // used in OLTP
    ///
    private String sideEffectKey;
    private boolean onGraphComputer = false;

    public GroupStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, HashMapSupplier.instance());
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> kvTraversal) {
        if ('k' == this.state) {
            this.keyTraversal = this.integrateChild(kvTraversal);
            this.state = 'v';
        } else if ('v' == this.state) {
            this.valueReduceTraversal = this.integrateChild(GroupStep.convertValueTraversal(kvTraversal));
            final List<Traversal.Admin<?, ?>> splitTraversal = GroupStep.splitOnBarrierStep(this.valueReduceTraversal);
            this.valueTraversal = this.integrateChild(splitTraversal.get(0));
            this.reduceTraversal = this.integrateChild(splitTraversal.get(1));
            this.state = 'x';
        } else {
            throw new IllegalStateException("The key and value traversals for group()-step have already been set: " + this);
        }
    }

    @Override
    public Optional<MemoryComputeKey> getMemoryComputeKey() {
        return Optional.of(MemoryComputeKey.of(this.getSideEffectKey(), GroupBiOperator.INSTANCE, false, false));
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        Map<K, Object> map = traverser.sideEffects(this.sideEffectKey);
        final K key = TraversalUtil.applyNullable(traverser, this.keyTraversal);
        final TraverserSet<Object> traverserSet = new TraverserSet<>();
        if (this.onGraphComputer) {
            this.valueTraversal.reset();
            this.valueTraversal.addStart(traverser);
            this.valueTraversal.getEndStep().forEachRemaining(t -> traverserSet.add((Traverser.Admin) t.asAdmin()));
            final TraverserSet<Object> set = (TraverserSet) map.get(key);
            if (null == set)
                map.put(key, traverserSet);
            else
                set.addAll(traverserSet);
        } else {
            traverserSet.add((Traverser.Admin) traverser.split());
            Traversal.Admin valueReduceTraversal = (Traversal.Admin) map.get(key);
            if (null == valueReduceTraversal) {
                valueReduceTraversal = this.valueReduceTraversal.clone();
                map.put(key, valueReduceTraversal);
            }
            traverserSet.forEach(valueReduceTraversal::addStart);
            // TODO: TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, valueReduceTraversal).ifPresent(Barrier::processAllStarts);
        }
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public void onGraphComputer() {
        this.onGraphComputer = true;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.sideEffectKey, this.keyTraversal, this.valueReduceTraversal);
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
            clone.keyTraversal = clone.integrateChild(this.keyTraversal.clone());
        clone.valueReduceTraversal = clone.integrateChild(this.valueReduceTraversal.clone());
        clone.valueTraversal = clone.integrateChild(this.valueTraversal.clone());
        clone.reduceTraversal = clone.integrateChild(this.reduceTraversal.clone());
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.sideEffectKey.hashCode();
        if (this.keyTraversal != null) result ^= this.keyTraversal.hashCode();
        result ^= this.valueReduceTraversal.hashCode();
        return result;
    }

    @Override
    public Object generateFinalResult(final Object object) {
        final Map<K, Object> map = (Map<K, Object>) object;
        final Map<K, Object> reducedMap = new HashMap<>();
        if (this.onGraphComputer) {
            for (final K key : map.keySet()) {
                final Traversal.Admin<?, V> reduceClone = this.reduceTraversal.clone();
                reduceClone.addStarts(((TraverserSet) map.get(key)).iterator());
                reducedMap.put(key, reduceClone.next());
            }
        } else
            map.forEach((key, traversal) -> reducedMap.put(key, ((Traversal.Admin) traversal).next()));
        return reducedMap;
    }

    /////////////////////////////

    private static <S, E> Traversal.Admin<S, E> convertValueTraversal(final Traversal.Admin<S, E> valueReduceTraversal) {
        if (valueReduceTraversal instanceof ElementValueTraversal ||
                valueReduceTraversal instanceof TokenTraversal ||
                valueReduceTraversal instanceof IdentityTraversal ||
                valueReduceTraversal.getStartStep() instanceof LambdaMapStep && ((LambdaMapStep) valueReduceTraversal.getStartStep()).getMapFunction() instanceof FunctionTraverser) {
            return (Traversal.Admin<S, E>) __.map(valueReduceTraversal).fold();
        } else {
            return valueReduceTraversal;
        }
    }

    private static List<Traversal.Admin<?, ?>> splitOnBarrierStep(final Traversal.Admin<?, ?> valueReduceTraversal) {
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

    /////////////////////////////

    public static final class GroupBiOperator<K, V> implements BinaryOperator<Map<K, V>>, Serializable {

        private static final GroupBiOperator INSTANCE = new GroupBiOperator();

        private GroupBiOperator() {

        }

        @Override
        public Map<K, V> apply(final Map<K, V> mutatingSeed, final Map<K, V> map) {
            for (final K key : map.keySet()) {
                TraverserSet traverserSet = (TraverserSet) mutatingSeed.get(key);
                if (null == traverserSet) {
                    traverserSet = new TraverserSet<>();
                    mutatingSeed.put(key, (V) traverserSet);
                }
                traverserSet.addAll((TraverserSet) map.get(key));
            }
            return mutatingSeed;
        }
    }
}
