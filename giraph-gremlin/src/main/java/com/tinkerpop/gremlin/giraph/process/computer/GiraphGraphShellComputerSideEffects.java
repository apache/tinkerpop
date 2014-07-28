package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.process.computer.SideEffects;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphShellComputerSideEffects implements SideEffects {

    private static final String COMPLETE_AND_IMMUTABLE = "The graph computation sideEffects are complete and immutable";
    protected static final String RUNTIME = "runtime";
    protected static final String ITERATION = "iteration";

    final Map<String, Object> sideEffects = new HashMap<>();

    public Set<String> keys() {
        return this.sideEffects.keySet();
    }

    public <R> R get(final String key) {
        return (R) this.sideEffects.get(key);
    }

    public void set(final String key, Object value) {
        this.sideEffects.put(key, value);
    }

    public int getIteration() {
        return (Integer) this.sideEffects.get(ITERATION);
    }

    public long getRuntime() {
        return (Long) this.sideEffects.get(RUNTIME);
    }

    public void setIfAbsent(final String key, final Object value) {
        throw new IllegalStateException();
    }

    public long incr(final String key, final long delta) {
        throw new IllegalStateException(COMPLETE_AND_IMMUTABLE);
    }

    public boolean and(final String key, final boolean bool) {
        throw new IllegalStateException(COMPLETE_AND_IMMUTABLE);
    }

    public boolean or(final String key, final boolean bool) {
        throw new IllegalStateException(COMPLETE_AND_IMMUTABLE);
    }
}
