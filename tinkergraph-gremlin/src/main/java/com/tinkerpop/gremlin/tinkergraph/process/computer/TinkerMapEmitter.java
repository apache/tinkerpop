package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.util.MapHelper;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerMapEmitter<K, V> implements MapReduce.MapEmitter<K, V> {

    public Map<K, Queue<V>> reduceMap = new ConcurrentHashMap<>();
    public Queue<Pair<K, V>> mapList = new ConcurrentLinkedQueue<>();
    private final boolean doReduce;

    public TinkerMapEmitter(final boolean doReduce) {
        this.doReduce = doReduce;
    }

    @Override
    public void emit(K key, V value) {
        if (this.doReduce)
            MapHelper.concurrentIncr(this.reduceMap, key, value);
        else
            this.mapList.add(new Pair<>(key, value));
    }
}
