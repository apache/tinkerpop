package com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupByStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupByComputerStep<S, K, V, R> extends FilterStep<S> implements SideEffectCapable, Reversible, VertexCentric, MapReducer {

    public java.util.Map<K, Collection<V>> groupMap;
    public final java.util.Map<K, R> reduceMap;
    public final SFunction<S, K> keyFunction;
    public final SFunction<S, V> valueFunction;
    public final SFunction<Collection<V>, R> reduceFunction;
    public final String variable;

    public GroupByComputerStep(final Traversal traversal, final GroupByStep groupByStep) {
        super(traversal);
        this.variable = groupByStep.variable;
        this.reduceMap = new HashMap<>();
        this.keyFunction = groupByStep.keyFunction;
        this.valueFunction = groupByStep.valueFunction == null ? s -> (V) s : groupByStep.valueFunction;
        this.reduceFunction = groupByStep.reduceFunction;
        this.setPredicate(traverser -> {
            doGroup(traverser.get(), this.groupMap, this.keyFunction, this.valueFunction);
            return true;
        });
        if (TraversalHelper.isLabeled(groupByStep))
            this.setAs(groupByStep.getAs());
    }

    public void setCurrentVertex(final Vertex vertex) {
        this.groupMap = vertex.<java.util.Map<K, Collection<V>>>property(Graph.Key.hidden(this.variable)).orElse(new HashMap<>());
        if (!vertex.property(Graph.Key.hidden(this.variable)).isPresent())
            vertex.property(Graph.Key.hidden(this.variable), this.groupMap);
    }

    private static <S, K, V> void doGroup(final S s, final java.util.Map<K, Collection<V>> groupMap, final SFunction<S, K> keyFunction, final SFunction<S, V> valueFunction) {
        final K key = keyFunction.apply(s);
        final V value = valueFunction.apply(s);
        Collection<V> values = groupMap.get(key);
        if (null == values) {
            values = new ArrayList<>();
            groupMap.put(key, values);
        }
        if (value instanceof Iterator) {
            while (((Iterator) value).hasNext()) {
                values.add(((Iterator<V>) value).next());
            }
        } else {
            values.add(value);
        }
    }

    public MapReduce getMapReduce() {
        return new GroupByComputerMapReduce(this);
    }

    public String getVariable() {
        return this.variable;
    }
}
