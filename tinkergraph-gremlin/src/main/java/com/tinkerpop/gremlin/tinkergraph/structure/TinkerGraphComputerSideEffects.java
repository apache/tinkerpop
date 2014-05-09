package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.structure.util.GraphVariableHelper;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphComputerSideEffects implements GraphComputer.SideEffects.Administrative {

    private final Map<String, Object> memory;
    private final AtomicInteger iteration = new AtomicInteger(0);
    private final AtomicLong runtime = new AtomicLong(0l);

    public TinkerGraphComputerSideEffects() {
        this(new ConcurrentHashMap<>());
    }

    public TinkerGraphComputerSideEffects(final Map<String, Object> state) {
        this.memory = state;
    }

    public Set<String> getVariables() {
        return this.memory.keySet();
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

    public <R> R get(final String variable) {
        return (R) this.memory.get(variable);
    }

    public long incr(final String variable, final long delta) {
        final Object value = this.memory.get(variable);
        final long incremented = value == null ? delta : (Long) value + delta;
        this.set(variable, incremented);
        return incremented;
    }

    public boolean and(final String variable, final boolean bool) {
        final boolean value = (Boolean) this.memory.getOrDefault(variable, bool);
        final boolean returnValue = value && bool;
        this.set(variable, returnValue);
        return returnValue;
    }

    public boolean or(final String variable, final boolean bool) {
        final boolean value = (Boolean) this.memory.getOrDefault(variable, bool);
        final boolean returnValue = value || bool;
        this.set(variable, returnValue);
        return returnValue;
    }

    public void setIfAbsent(final String variable, final Object value) {
        if (this.memory.containsKey(variable))
            throw new IllegalStateException("The memory already has a value for key " + variable);
        this.set(variable, value);
    }

    public void set(final String variable, final Object value) {
        GraphVariableHelper.validateVariable(variable, value);
        this.memory.put(variable, value);
    }
}
