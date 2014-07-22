package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.util.MapHelper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerMapEmitter<K, V> implements MapReduce.MapEmitter<K, V> {

    public Map<K, List<V>> reduceMap = new HashMap<>();

    public void emit(K key, V value) {
        MapHelper.incr(this.reduceMap, key, value);
    }
}
