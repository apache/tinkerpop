package com.tinkerpop.gremlin.oltp.sideeffect;

import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.map.MapPipe;
import com.tinkerpop.gremlin.util.GremlinHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupByPipe<S, K, V, R> extends MapPipe<S, S> {

    public final Map<K, Collection<V>> groupMap;
    public final Map<K, R> reduceMap;
    public final Function<S, K> keyFunction;
    public final Function<S, V> valueFunction;
    public final Function<Collection<V>, R> reduceFunction;

    public GroupByPipe(final Pipeline pipeline, final String variable, final Function<S, K> keyFunction, final Function<S, V> valueFunction, final Function<Collection<V>, R> reduceFunction) {
        super(pipeline);
        this.groupMap = GremlinHelper.getOrCreate(this.pipeline, variable, HashMap<K, Collection<V>>::new);
        this.reduceMap = new HashMap<>();
        this.keyFunction = keyFunction;
        this.valueFunction = valueFunction == null ? s -> (V) s : valueFunction;
        this.reduceFunction = reduceFunction;
        this.setFunction(holder -> {
            doGroup(holder.get(), this.groupMap, this.keyFunction, this.valueFunction);
            if (null != reduceFunction && !this.getPreviousPipe().hasNext()) {
                doReduce(this.groupMap, this.reduceMap, this.reduceFunction);
                this.pipeline.set(variable, this.reduceMap);
            }
            return holder.get();
        });
    }

    public GroupByPipe(final Pipeline pipeline, final String variable, final Function<S, K> keyFunction, final Function<S, V> valueFunction) {
        this(pipeline, variable, keyFunction, valueFunction, null);
    }


    public GroupByPipe(final Pipeline pipeline, final String variable, final Function<S, K> keyFunction) {
        this(pipeline, variable, keyFunction, null, null);
    }

    public static <S, K, V> void doGroup(final S s, final Map<K, Collection<V>> groupMap, final Function<S, K> keyFunction, final Function<S, V> valueFunction) {
        final K key = keyFunction.apply(s);
        final V value = valueFunction.apply(s);
        Collection<V> values = groupMap.get(key);
        if (null == values) {
            values = new ArrayList<>();
            groupMap.put(key, values);
        }
        GroupByPipe.addValue(value, values);
    }

    public static <K, V, R> void doReduce(final Map<K, Collection<V>> groupMap, final Map<K, R> reduceMap, final Function<Collection<V>, R> reduceFunction) {
        groupMap.forEach((k, vv) -> {
            reduceMap.put(k, (R) reduceFunction.apply(vv));
        });
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
}
