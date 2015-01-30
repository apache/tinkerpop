package com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectRegistrar;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.mapreduce.GroupMapReduce;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.BulkSet;
import com.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.process.traversal.util.TraversalUtil;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupStep<S, K, V, R> extends SideEffectStep<S> implements SideEffectCapable, SideEffectRegistrar, TraversalHolder, Reversible, EngineDependent, MapReducer<Object, Collection, Object, Object, Map> {

    private char state = 'k';
    private Traversal.Admin<S, K> keyFunction = new IdentityTraversal<>();
    private Traversal.Admin<S, V> valueFunction = new IdentityTraversal<>();
    private Traversal.Admin<Collection<V>, R> reduceFunction = null;
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

    private static <S, K, V> void doGroup(final Traverser.Admin<S> traverser, final Map<K, Collection<V>> groupMap, final Traversal.Admin<S, K> keyFunction, final Traversal.Admin<S, V> valueFunction) {
        final K key = TraversalUtil.function(traverser, keyFunction);
        final V value = TraversalUtil.function(traverser, valueFunction);
        Collection<V> values = groupMap.get(key);
        if (null == values) {
            values = new BulkSet<>();
            groupMap.put(key, values);
        }
        TraversalHelper.addToCollectionUnrollIterator(values, value, traverser.bulk());
    }

    private static <K, V, R> void doReduce(final Map<K, Collection<V>> groupMap, final Map<K, R> reduceMap, final Traversal.Admin<Collection<V>, R> reduceFunction) {
        groupMap.forEach((k, vv) -> reduceMap.put(k, TraversalUtil.function(vv, reduceFunction)));
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
        return TraversalHelper.makeStepString(this, this.sideEffectKey, this.keyFunction, this.valueFunction, this.reduceFunction);
    }

    @Override
    public <A, B> List<Traversal<A, B>> getLocalTraversals() {
        return null == this.reduceFunction ? (List) Arrays.asList(this.keyFunction, this.valueFunction) : (List) Arrays.asList(this.keyFunction, this.valueFunction, this.reduceFunction);
    }

    public Traversal.Admin<Collection<V>, R> getReduceFunction() {
        return this.reduceFunction;
    }

    @Override
    public void addLocalTraversal(final Traversal.Admin<?, ?> kvrTraversal) {
        if ('k' == this.state) {
            this.keyFunction = (Traversal.Admin<S, K>) kvrTraversal;
            this.executeTraversalOperations(this.keyFunction, TYPICAL_LOCAL_OPERATIONS);
            this.state = 'v';
        } else if ('v' == this.state) {
            this.valueFunction = (Traversal.Admin<S, V>) kvrTraversal;
            this.executeTraversalOperations(this.valueFunction, TYPICAL_LOCAL_OPERATIONS);
            this.state = 'r';
        } else if ('r' == this.state) {
            this.reduceFunction = (Traversal.Admin<Collection<V>, R>) kvrTraversal;
            this.executeTraversalOperations(this.reduceFunction, TYPICAL_LOCAL_OPERATIONS);
            this.state = 'x';
        } else {
            throw new IllegalStateException("The key, value, and reduce functions for group()-step have already been set");
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
        clone.keyFunction = this.keyFunction.clone().asAdmin();
        clone.executeTraversalOperations(this.keyFunction, TYPICAL_LOCAL_OPERATIONS);
        clone.valueFunction = this.valueFunction.clone().asAdmin();
        clone.executeTraversalOperations(this.valueFunction, TYPICAL_LOCAL_OPERATIONS);
        if (null != this.reduceFunction) {
            clone.reduceFunction = this.reduceFunction.clone().asAdmin();
            clone.executeTraversalOperations(this.reduceFunction, TYPICAL_LOCAL_OPERATIONS);
        }
        GroupStep.generateConsumer(clone);
        return clone;
    }

    /////////////////////////

    private static final <S, K, V, R> void generateConsumer(final GroupStep<S, K, V, R> groupStep) {
        groupStep.setConsumer(traverser -> {
            final Map<K, Collection<V>> groupByMap = null == groupStep.tempGroupByMap ? traverser.sideEffects(groupStep.sideEffectKey) : groupStep.tempGroupByMap; // for nested traversals and not !starts.hasNext()
            doGroup(traverser.asAdmin(), groupByMap, groupStep.keyFunction, groupStep.valueFunction);
            if (!groupStep.onGraphComputer && null != groupStep.reduceFunction && !groupStep.starts.hasNext()) {
                groupStep.tempGroupByMap = groupByMap;
                final Map<K, R> reduceMap = new HashMap<>();
                doReduce(groupByMap, reduceMap, groupStep.reduceFunction);
                traverser.sideEffects(groupStep.sideEffectKey, reduceMap);
            }
        });
    }
}
