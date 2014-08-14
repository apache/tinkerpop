package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.SideEffects;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerSideEffects implements SideEffects.Administrative {

    public final Set<String> sideEffectKeys = new HashSet<>();
    public final Map<String, Object> sideEffectsMap;
    private final AtomicInteger iteration = new AtomicInteger(0);
    private final AtomicLong runtime = new AtomicLong(0l);


    public TinkerSideEffects(final VertexProgram vertexProgram, final List<MapReduce> mapReducers) {
        this.sideEffectsMap = new ConcurrentHashMap<>();
        if (null != vertexProgram) {
            this.sideEffectKeys.addAll(vertexProgram.getSideEffectComputeKeys());
        }
        for (final MapReduce mapReduce : mapReducers) {
            this.sideEffectKeys.add(mapReduce.getSideEffectKey());
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
        this.runtime.set(runTime);
    }

    public long getRuntime() {
        return this.runtime.get();
    }

    protected void complete() {
        this.iteration.decrementAndGet();
    }

    public boolean isInitialIteration() {
        return this.getIteration() == 0;
    }

    public <R> Optional<R> get(final String key) {
        return Optional.ofNullable((R) this.sideEffectsMap.get(key));
    }

    public long incr(final String key, final long delta) {
        checkKey(key);
        final Object value = this.sideEffectsMap.get(key);
        final long incremented = value == null ? delta : (Long) value + delta;
        this.set(key, incremented);
        return incremented;
    }

    public boolean and(final String key, final boolean bool) {
        checkKey(key);
        final boolean value = (Boolean) this.sideEffectsMap.getOrDefault(key, bool);
        final boolean returnValue = value && bool;
        this.set(key, returnValue);
        return returnValue;
    }

    public boolean or(final String key, final boolean bool) {
        checkKey(key);
        final boolean value = (Boolean) this.sideEffectsMap.getOrDefault(key, bool);
        final boolean returnValue = value || bool;
        this.set(key, returnValue);
        return returnValue;
    }

    public void set(final String key, final Object value) {
        checkKey(key);
        GraphVariableHelper.validateVariable(key, value);
        this.sideEffectsMap.put(key, value);
    }

    public String toString() {
        return StringFactory.computerSideEffectsString(this);
    }

    private void checkKey(final String key) {
        if (!this.sideEffectKeys.contains(key))
            throw GraphComputer.Exceptions.providedKeyIsNotASideEffectKey(key);
    }
}
