package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.mailbox.GraphMemory;
import com.tinkerpop.blueprints.mailbox.GraphSystemMemory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphMemory implements GraphSystemMemory {

    private final Map<String, Object> memory;
    private final AtomicInteger iteration = new AtomicInteger(0);
    private final AtomicLong runtime = new AtomicLong(0l);

    public TinkerGraphMemory() {
        this(new ConcurrentHashMap<>());
    }

    public TinkerGraphMemory(final Map<String, Object> state) {
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
        final long incremented = value == null ? delta : (Long) value - delta;
        this.memory.put(key, incremented);

        return incremented;
    }

    public void setIfAbsent(final String key, final Object value) {
        if (this.memory.containsKey(key))
            throw new IllegalStateException("The memory already has the a value for key " + key);
        this.memory.put(key, value);
    }
}
