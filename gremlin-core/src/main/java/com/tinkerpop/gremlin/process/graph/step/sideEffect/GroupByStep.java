package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.GroupByMapReduce;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupByStep<S, K, V, R> extends SideEffectStep<S> implements SideEffectCapable, Reversible, VertexCentric, MapReducer<Object, Collection, Object, Object, Map> {

    public Map<K, Collection<V>> groupByMap;
    public final Map<K, R> reduceMap;
    public final SFunction<Traverser<S>, K> keyFunction;
    public final SFunction<Traverser<S>, V> valueFunction;
    public final SFunction<Collection<V>, R> reduceFunction;
    private final String sideEffectKey;
    private final String hiddenSideEffectKey;
    public boolean vertexCentric = false;

    public GroupByStep(final Traversal traversal, final String sideEffectKey, final SFunction<Traverser<S>, K> keyFunction, final SFunction<Traverser<S>, V> valueFunction, final SFunction<Collection<V>, R> reduceFunction) {
        super(traversal);
        this.sideEffectKey = null == sideEffectKey ? this.getLabel() : sideEffectKey;
        this.hiddenSideEffectKey = Graph.Key.hide(this.sideEffectKey);
        TraversalHelper.verifySideEffectKeyIsNotAStepLabel(this.sideEffectKey, this.traversal);
        this.groupByMap = this.traversal.sideEffects().getOrCreate(this.sideEffectKey, HashMap<K, Collection<V>>::new);
        this.reduceMap = new HashMap<>();
        this.keyFunction = keyFunction;
        this.valueFunction = null == valueFunction ? s -> (V) s.get() : valueFunction;
        this.reduceFunction = reduceFunction;
        this.setConsumer(traverser -> {
            doGroup(traverser, this.groupByMap, this.keyFunction, this.valueFunction);
            if (!this.vertexCentric) {
                if (null != reduceFunction && !this.starts.hasNext()) {
                    doReduce(this.groupByMap, this.reduceMap, this.reduceFunction);
                    this.traversal.sideEffects().set(this.sideEffectKey, this.reduceMap);
                }
            }
        });
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    private static <S, K, V> void doGroup(final Traverser<S> traverser, final Map<K, Collection<V>> groupMap, final SFunction<Traverser<S>, K> keyFunction, final SFunction<Traverser<S>, V> valueFunction) {
        final K key = keyFunction.apply(traverser);
        final V value = valueFunction.apply(traverser);
        Collection<V> values = groupMap.get(key);
        if (null == values) {
            values = new ArrayList<>();
            groupMap.put(key, values);
        }
        GroupByStep.addValue(value, values);
    }

    private static <K, V, R> void doReduce(final Map<K, Collection<V>> groupMap, final Map<K, R> reduceMap, final SFunction<Collection<V>, R> reduceFunction) {
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
    public void setCurrentVertex(final Vertex vertex) {
        this.vertexCentric = true;
        this.groupByMap = vertex.<java.util.Map<K, Collection<V>>>property(this.hiddenSideEffectKey).orElse(new HashMap<>());
        if (!vertex.property(this.hiddenSideEffectKey).isPresent())
            vertex.property(this.hiddenSideEffectKey, this.groupByMap);
    }

    @Override
    public MapReduce<Object, Collection, Object, Object, Map> getMapReduce() {
        return new GroupByMapReduce(this);
    }

    @Override
    public String toString() {
        return Graph.Key.isHidden(this.sideEffectKey) ? super.toString() : TraversalHelper.makeStepString(this, this.sideEffectKey);
    }
}
