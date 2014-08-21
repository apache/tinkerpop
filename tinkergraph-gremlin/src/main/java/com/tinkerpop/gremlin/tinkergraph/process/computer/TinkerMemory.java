package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.MemoryHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerMemory implements Memory.Administrative {

    public final Set<String> sideEffectKeys = new HashSet<>();
    public final Map<String, Object> sideEffectsMap;
    private final AtomicInteger iteration = new AtomicInteger(0);
    private final AtomicLong runtime = new AtomicLong(0l);
    private boolean complete = false;

    public TinkerMemory(final VertexProgram vertexProgram, final List<MapReduce> mapReducers) {
        this.sideEffectsMap = new ConcurrentHashMap<>();
        if (null != vertexProgram) {
            for (final String key : (Set<String>) vertexProgram.getMemoryComputeKeys()) {
                MemoryHelper.validateKey(key);
                this.sideEffectKeys.add(key);
            }
        }
        for (final MapReduce mapReduce : mapReducers) {
            this.sideEffectKeys.add(mapReduce.getMemoryKey());
        }
    }

    public Set<String> keys() {
        return this.sideEffectsMap.keySet();
    }

    public void incrIteration() {
        this.iteration.getAndIncrement();
    }

    public int getIteration() {
        return this.iteration.get();
    }

    public void setRuntime(final long runTime) {
        if (this.complete) throw Memory.Exceptions.memoryCompleteAndImmutable();
        this.runtime.set(runTime);
    }

    public long getRuntime() {
        return this.runtime.get();
    }

    protected void complete() {
        this.iteration.decrementAndGet();
        this.complete = true;
    }

    public boolean isInitialIteration() {
        return this.getIteration() == 0;
    }

    public <R> R get(final String key) throws IllegalArgumentException {
        final R r = (R) this.sideEffectsMap.get(key);
        if (null == r)
            throw Memory.Exceptions.memoryDoesNotExist(key);
        else
            return r;
    }

    public long incr(final String key, final long delta) {
        checkKeyValue(key, delta);
        final Object value = this.sideEffectsMap.get(key);
        final long returnValue = value == null ? delta : (Long) value + delta;
        this.sideEffectsMap.put(key, returnValue);
        return returnValue;
    }

    public boolean and(final String key, final boolean bool) {
        checkKeyValue(key, bool);
        final boolean value = (Boolean) this.sideEffectsMap.getOrDefault(key, bool);
        final boolean returnValue = value && bool;
        this.sideEffectsMap.put(key, returnValue);
        return returnValue;
    }

    public boolean or(final String key, final boolean bool) {
        checkKeyValue(key, bool);
        final boolean value = (Boolean) this.sideEffectsMap.getOrDefault(key, bool);
        final boolean returnValue = value || bool;
        this.sideEffectsMap.put(key, returnValue);
        return returnValue;
    }

    public void set(final String key, final Object value) {
        checkKeyValue(key, value);
        this.sideEffectsMap.put(key, value);
    }

    public String toString() {
        return StringFactory.computeMemoryString(this);
    }

    private void checkKeyValue(final String key, final Object value) {
        if (this.complete) throw Memory.Exceptions.memoryCompleteAndImmutable();
        if (!this.sideEffectKeys.contains(key))
            throw GraphComputer.Exceptions.providedKeyIsNotAMemoryKey(key);
        MemoryHelper.validateValue(value);
    }
}
