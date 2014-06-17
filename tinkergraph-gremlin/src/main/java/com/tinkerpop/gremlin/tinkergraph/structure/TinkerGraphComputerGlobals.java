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
public class TinkerGraphComputerGlobals implements GraphComputer.Globals.Administrative {

    private final Map<String, Object> globals;
    private final AtomicInteger iteration = new AtomicInteger(0);
    private final AtomicLong runtime = new AtomicLong(0l);

    public TinkerGraphComputerGlobals() {
        this(new ConcurrentHashMap<>());
    }

    public TinkerGraphComputerGlobals(final Map<String, Object> state) {
        this.globals = state;
    }

    public Set<String> keys() {
        return this.globals.keySet();
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
        return (R) this.globals.get(key);
    }

    public long incr(final String key, final long delta) {
        final Object value = this.globals.get(key);
        final long incremented = value == null ? delta : (Long) value + delta;
        this.set(key, incremented);
        return incremented;
    }

    public boolean and(final String key, final boolean bool) {
        final boolean value = (Boolean) this.globals.getOrDefault(key, bool);
        final boolean returnValue = value && bool;
        this.set(key, returnValue);
        return returnValue;
    }

    public boolean or(final String key, final boolean bool) {
        final boolean value = (Boolean) this.globals.getOrDefault(key, bool);
        final boolean returnValue = value || bool;
        this.set(key, returnValue);
        return returnValue;
    }

    public void setIfAbsent(final String key, final Object value) {
        if (!this.globals.containsKey(key))
            this.set(key, value);
    }

    public void set(final String key, final Object value) {
        GraphVariableHelper.validateVariable(key, value);
        this.globals.put(key, value);
    }
}
