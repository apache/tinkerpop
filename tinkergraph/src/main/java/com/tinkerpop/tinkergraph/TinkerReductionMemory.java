package com.tinkerpop.tinkergraph;

import com.tinkerpop.blueprints.computer.GraphMemory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerReductionMemory<K, V, R> implements GraphMemory.ReductionMemory<K, V, R> {

    protected final Map<K, Queue<V>> map = new ConcurrentHashMap<>();
    protected Map<K, R> reducedMap = null;
    protected final BiFunction<K, Iterator<V>, R> reduction;

    public TinkerReductionMemory(final BiFunction<K, Iterator<V>, R> reduction) {
        this.reduction = reduction;
    }

    public void emit(final K key, final V value) {
        final Queue<V> queue = this.map.getOrDefault(key, new ConcurrentLinkedQueue<>());
        this.map.put(key, queue);
        queue.add(value);
    }

    public Optional<R> get(final K key) {
        return Optional.ofNullable(this.reducedMap.get(key));
    }

    public void reduce() {
        if (null != this.reducedMap)
            throw new IllegalStateException("The reduction memory has already been reduced");

        this.reducedMap = new HashMap<>();
        this.map.forEach((key, values) -> {
            this.reducedMap.put(key, this.reduction.apply(key, values.iterator()));
        });
        this.map.clear();
    }

    public Iterator<K> getKeys() {
        return this.reducedMap.keySet().iterator();
    }
}
