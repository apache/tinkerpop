package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.Constants;
import com.tinkerpop.gremlin.process.computer.SideEffects;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphImmutableSideEffects implements SideEffects {

    private long runtime = 0l;
    private int iteration = -1;
    private final Map<String, Object> sideEffectsMap = new HashMap<>();
    private boolean complete = false;

    public Set<String> keys() {
        return this.sideEffectsMap.keySet();
    }

    public <R> Optional<R> get(final String key) {
        return Optional.ofNullable((R) this.sideEffectsMap.get(key));
    }

    public void set(final String key, Object value) {
        if (this.complete) throw SideEffects.Exceptions.sideEffectsCompleteAndImmutable();
        this.sideEffectsMap.put(key, value);
    }

    public int getIteration() {
        return this.iteration;
    }

    public long getRuntime() {
        return this.runtime;
    }

    protected void complete(final long runtime) {
        this.complete = true;
        this.runtime = runtime;
        if (this.sideEffectsMap.containsKey(Constants.ITERATION))
            this.iteration = (int) this.sideEffectsMap.remove(Constants.ITERATION);
    }

    public long incr(final String key, final long delta) {
        throw SideEffects.Exceptions.sideEffectsCompleteAndImmutable();
    }

    public boolean and(final String key, final boolean bool) {
        throw SideEffects.Exceptions.sideEffectsCompleteAndImmutable();
    }

    public boolean or(final String key, final boolean bool) {
        throw SideEffects.Exceptions.sideEffectsCompleteAndImmutable();
    }

    public String toString() {
        return StringFactory.computerSideEffectsString(this);
    }
}
