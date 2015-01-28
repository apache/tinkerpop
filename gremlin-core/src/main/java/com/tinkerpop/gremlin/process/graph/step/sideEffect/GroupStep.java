package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.graph.marker.FunctionHolder;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectRegistrar;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.GroupMapReduce;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.BulkSet;
import com.tinkerpop.gremlin.process.util.SmartLambda;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.CloneableLambda;
import com.tinkerpop.gremlin.util.function.ResettableLambda;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupStep<S, K, V, R> extends SideEffectStep<S> implements SideEffectCapable, SideEffectRegistrar, TraversalHolder, FunctionHolder<Object, Object>, Reversible, EngineDependent, MapReducer<Object, Collection, Object, Object, Map> {

    private char state = 'k';
    private SmartLambda<S, K> keyFunction = new SmartLambda<>();
    private SmartLambda<S, V> valueFunction = new SmartLambda<>();
    private Function<Collection<V>, R> reduceFunction = null;
    private String sideEffectKey;
    private boolean onGraphComputer = false;
    private Map<K, Collection<V>> tempGroupByMap;

    public GroupStep(final Traversal traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        GroupStep.generateConsumer(this);
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public void registerSideEffects() {
        if (this.sideEffectKey == null) this.sideEffectKey = this.getId();
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, HashMap<K, Collection<V>>::new);
    }

    private static <S, K, V> void doGroup(final Traverser<S> traverser, final Map<K, Collection<V>> groupMap, final SmartLambda<S, K> keyFunction, final SmartLambda<S, V> valueFunction) {
        final K key = keyFunction.apply((S) traverser);
        final V value = valueFunction.apply((S) traverser);
        Collection<V> values = groupMap.get(key);
        if (null == values) {
            values = new BulkSet<>();
            groupMap.put(key, values);
        }
        TraversalHelper.addToCollectionUnrollIterator(values, value, traverser.bulk());
    }

    private static <K, V, R> void doReduce(final Map<K, Collection<V>> groupMap, final Map<K, R> reduceMap, final Function<Collection<V>, R> reduceFunction) {
        groupMap.forEach((k, vv) -> reduceMap.put(k, reduceFunction.apply(vv)));
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        this.onGraphComputer = traversalEngine.equals(TraversalEngine.COMPUTER);
    }

    @Override
    public MapReduce<Object, Collection, Object, Object, Map> getMapReduce() {
        return new GroupMapReduce(this);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey);
    }

    @Override
    public List<Traversal<S, Object>> getLocalTraversals() {
        final List<Traversal<S, Object>> traversals = new ArrayList<>();
        traversals.addAll((List) this.keyFunction.getTraversalAsList());
        traversals.addAll((List) this.valueFunction.getTraversalAsList());
        return traversals;
    }

    @Override
    public void reset() {
        super.reset();
        this.keyFunction.reset();
        this.valueFunction.reset();
        ResettableLambda.tryReset(this.reduceFunction);
    }

    public Function<Collection<V>, R> getReduceFunction() {
        return this.reduceFunction;
    }

    @Override
    public void addFunction(final Function<Object, Object> function) {
        if ('k' == this.state) {
            this.keyFunction.setLambda(function);
            this.executeTraversalOperations(this.keyFunction.getTraversal(), TYPICAL_LOCAL_OPERATIONS);
            this.state = 'v';
        } else if ('v' == this.state) {
            this.valueFunction.setLambda(function);
            this.executeTraversalOperations(this.valueFunction.getTraversal(), TYPICAL_LOCAL_OPERATIONS);
            this.state = 'r';
        } else if ('r' == this.state) {
            this.reduceFunction = (Function) function;
            this.state = 'x';
        } else {
            throw new IllegalStateException("The key, value, and reduce functions for group()-step have already been set");
        }
    }

    @Override
    public List<Function<Object, Object>> getFunctions() {
        if (this.state == 'k') {
            return Collections.emptyList();
        } else if (this.state == 'v') {
            return Arrays.asList((Function) this.keyFunction);
        } else if (this.state == 'r') {
            return Arrays.asList((Function) this.keyFunction, (Function) this.valueFunction);
        } else {
            return Arrays.asList((Function) this.keyFunction, (Function) this.valueFunction, (Function) this.reduceFunction);
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = TraversalHolder.super.getRequirements();
        requirements.add(TraverserRequirement.BULK);
        requirements.add(TraverserRequirement.SIDE_EFFECTS);
        return requirements;
    }

    @Override
    public GroupStep<S, K, V, R> clone() throws CloneNotSupportedException {
        final GroupStep<S, K, V, R> clone = (GroupStep<S, K, V, R>) super.clone();
        clone.keyFunction = this.keyFunction.clone();
        clone.executeTraversalOperations(this.keyFunction.getTraversal(), TYPICAL_LOCAL_OPERATIONS);
        clone.valueFunction = this.valueFunction.clone();
        clone.executeTraversalOperations(this.valueFunction.getTraversal(), TYPICAL_LOCAL_OPERATIONS);
        clone.reduceFunction = CloneableLambda.tryClone(this.reduceFunction);
        GroupStep.generateConsumer(clone);
        return clone;
    }

    /////////////////////////

    private static final <S, K, V, R> void generateConsumer(final GroupStep<S, K, V, R> groupStep) {
        groupStep.setConsumer(traverser -> {
            final Map<K, Collection<V>> groupByMap = null == groupStep.tempGroupByMap ? traverser.sideEffects(groupStep.sideEffectKey) : groupStep.tempGroupByMap; // for nested traversals and not !starts.hasNext()
            doGroup(traverser, groupByMap, groupStep.keyFunction, groupStep.valueFunction);
            if (!groupStep.onGraphComputer && null != groupStep.reduceFunction && !groupStep.starts.hasNext()) {
                groupStep.tempGroupByMap = groupByMap;
                final Map<K, R> reduceMap = new HashMap<>();
                doReduce(groupByMap, reduceMap, groupStep.reduceFunction);
                traverser.sideEffects(groupStep.sideEffectKey, reduceMap);
            }
        });
    }
}
