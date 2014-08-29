package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.util.MapHelper;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerMapEmitter<K, V> implements MapReduce.MapEmitter<K, V> {

    public Map<K, List<V>> reduceMap = new HashMap<>();
    public List<Pair<K, V>> mapList = new ArrayList();
    private final boolean doReduce;

    public TinkerMapEmitter(final boolean doReduce) {
        this.doReduce = doReduce;
    }

    @Override
    public void emit(K key, V value) {
        if (this.doReduce)
            MapHelper.incr(this.reduceMap, key, value);
        else
            this.mapList.add(new Pair<>(key, value));
    }
}
