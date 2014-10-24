package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.util.MapHelper;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerMapEmitter<K, V> implements MapReduce.MapEmitter<K, V> {

    public Map<K, Queue<V>> reduceMap;
    public Queue<Pair<K, V>> mapQueue;
    private final boolean doReduce;

    public TinkerMapEmitter(final boolean doReduce) {
        this.doReduce = doReduce;
        if (this.doReduce)
            this.reduceMap = new ConcurrentHashMap<>();
        else
            this.mapQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void emit(K key, V value) {
        if (this.doReduce)
            MapHelper.concurrentIncr(this.reduceMap, key, value);
        else
            this.mapQueue.add(new Pair<>(key, value));
    }

    protected void complete(final MapReduce<K, V, ?, ?, ?> mapReduce) {
        if (!this.doReduce && mapReduce.getMapKeySort().isPresent()) {
            final Comparator<K> comparator = mapReduce.getMapKeySort().get();
            final List<Pair<K, V>> list = new ArrayList<>(this.mapQueue);
            Collections.sort(list, Comparator.comparing(Pair::getValue0, comparator));
            this.mapQueue.clear();
            this.mapQueue.addAll(list);
        } else if (mapReduce.getMapKeySort().isPresent()) {
            final Comparator<K> comparator = mapReduce.getMapKeySort().get();
            final List<Map.Entry<K, Queue<V>>> list = new ArrayList<>();
            list.addAll(this.reduceMap.entrySet());
            Collections.sort(list, Comparator.comparing(Map.Entry::getKey, comparator));
            this.reduceMap = new LinkedHashMap<>();
            list.forEach(entry -> this.reduceMap.put(entry.getKey(), entry.getValue()));
        }
    }
}
