package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.GroupByMapReduce;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupByStep<S, K, V, R> extends SideEffectStep<S> implements SideEffectCapable, Reversible, EngineDependent, MapReducer<Object, Collection, Object, Object, Map> {

    private final Map<K, R> reduceMap;
    private final Function<Traverser<S>, K> keyFunction;
    private final Function<Traverser<S>, V> valueFunction;
    private final Function<Collection<V>, R> reduceFunction;
    private final String sideEffectKey;
    private boolean vertexCentric = false;

    public GroupByStep(final Traversal traversal, final String sideEffectKey, final Function<Traverser<S>, K> keyFunction, final Function<Traverser<S>, V> valueFunction, final Function<Collection<V>, R> reduceFunction) {
        super(traversal);
        this.sideEffectKey = null == sideEffectKey ? this.getLabel() : sideEffectKey;
        TraversalHelper.verifySideEffectKeyIsNotAStepLabel(this.sideEffectKey, this.traversal);
        this.reduceMap = new HashMap<>();
        this.keyFunction = keyFunction;
        this.valueFunction = null == valueFunction ? s -> (V) s.get() : valueFunction;
        this.reduceFunction = reduceFunction;
        this.setConsumer(traverser -> {
            Map<K, Collection<V>> groupByMap = this.traversal.sideEffects().getOrCreate(this.sideEffectKey, HashMap<K, Collection<V>>::new);
            doGroup(traverser, groupByMap, this.keyFunction, this.valueFunction);
            if (!this.vertexCentric) {
                if (null != reduceFunction && !this.starts.hasNext()) {
                    doReduce(groupByMap, this.reduceMap, this.reduceFunction);
                    this.traversal.sideEffects().set(this.sideEffectKey, this.reduceMap);
                }
            }
        });
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    private static <S, K, V> void doGroup(final Traverser<S> traverser, final Map<K, Collection<V>> groupMap, final Function<Traverser<S>, K> keyFunction, final Function<Traverser<S>, V> valueFunction) {
        final K key = keyFunction.apply(traverser);
        final V value = valueFunction.apply(traverser);
        Collection<V> values = groupMap.get(key);
        if (null == values) {
            values = new ArrayList<>();
            groupMap.put(key, values);
        }
        for (int i = 0; i < traverser.getBulk(); i++)
            GroupByStep.addValue(value, values);
    }

    private static <K, V, R> void doReduce(final Map<K, Collection<V>> groupMap, final Map<K, R> reduceMap, final Function<Collection<V>, R> reduceFunction) {
        groupMap.forEach((k, vv) -> reduceMap.put(k, reduceFunction.apply(vv)));
    }

    public static void addValue(final Object value, final Collection values) {
        if (value instanceof Iterator) {
            while (((Iterator) value).hasNext()) {
                values.add(((Iterator) value).next());
            }
        } else {
            values.add(value);
        }
    }

    @Override
    public void onEngine(final Engine engine) {
        this.vertexCentric = engine.equals(Engine.COMPUTER);
    }

    @Override
    public MapReduce<Object, Collection, Object, Object, Map> getMapReduce() {
        return new GroupByMapReduce(this);
    }

    @Override
    public String toString() {
        return Graph.Key.isHidden(this.sideEffectKey) ? super.toString() : TraversalHelper.makeStepString(this, this.sideEffectKey);
    }

    public Function<Collection<V>, R> getReduceFunction() {
        return this.reduceFunction;
    }
}
