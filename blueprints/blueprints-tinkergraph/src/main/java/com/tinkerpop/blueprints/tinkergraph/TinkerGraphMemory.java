package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.computer.GraphSystemMemory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphMemory implements GraphSystemMemory {

    private final Graph graph;
    private final Map<String, Object> memory;
    private final AtomicInteger iteration = new AtomicInteger(0);
    private final AtomicLong runtime = new AtomicLong(0l);
    protected TinkerReductionMemory reductionMemory = null;

    public TinkerGraphMemory(final Graph graph) {
        this(graph, new ConcurrentHashMap<>());
    }

    public TinkerGraphMemory(final Graph graph, final Map<String, Object> state) {
        this.graph = graph;
        this.memory = state;
    }

    public void incrIteration() {
        this.iteration.getAndIncrement();
    }

    public int getIteration() {
        return this.iteration.get();
    }

    public void setRuntime(final long runTime) {
        this.runtime.set(runTime);
    }

    public long getRuntime() {
        return this.runtime.get();
    }

    public boolean isInitialIteration() {
        return this.getIteration() == 0;
    }

    public <R> R get(final String key) {
        return (R) this.memory.get(key);
    }

    public long increment(final String key, final long delta) {
        final Object value = this.memory.get(key);
        final long incremented = value == null ? delta : (Long) value + delta;
        this.memory.put(key, incremented);
        return incremented;
    }

    public long decrement(final String key, final long delta) {
        final Object value = this.memory.get(key);
        final long decremented = value == null ? delta : (Long) value - delta;
        this.memory.put(key, decremented);
        return decremented;
    }

    public boolean and(final String key, final boolean bool) {
        final boolean value = (Boolean) this.memory.getOrDefault(key, bool);
        final boolean returnValue = value & bool;
        this.memory.put(key, returnValue);
        return returnValue;
    }

    public boolean or(final String key, final boolean bool) {
        final boolean value = (Boolean) this.memory.getOrDefault(key, bool);
        final boolean returnValue = value | bool;
        this.memory.put(key, returnValue);
        return returnValue;
    }

    public void setIfAbsent(final String key, final Object value) {
        if (this.memory.containsKey(key))
            throw new IllegalStateException("The memory already has the a value for key " + key);
        this.memory.put(key, value);
    }

    public Graph getGraph() {
        return this.graph;
    }

    public <K, V, R> ReductionMemory<K, V, R> getReductionMemory() {
        return (ReductionMemory) this.reductionMemory;
    }
}
