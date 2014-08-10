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
public class GiraphGraphShellComputerSideEffects implements SideEffects {

    private static final String COMPLETE_AND_IMMUTABLE = "The graph computation sideEffects are complete and immutable";
    private long runtime = 0l;
    private int iteration = -1;
    private final Map<String, Object> sideEffectsMap = new HashMap<>();

    public Set<String> keys() {
        return this.sideEffectsMap.keySet();
    }

    public <R> Optional<R> get(final String key) {
        return Optional.ofNullable((R) this.sideEffectsMap.get(key));
    }

    public void set(final String key, Object value) {
        this.sideEffectsMap.put(key, value);
    }

    public int getIteration() {
        return this.iteration;
    }

    public long getRuntime() {
        return this.runtime;
    }

    protected void complete(final long runtime) {
        this.runtime = runtime;
        if (this.sideEffectsMap.containsKey(Constants.ITERATION))
            this.iteration = (int) this.sideEffectsMap.remove(Constants.ITERATION);
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

    public String toString() {
        return StringFactory.computerSideEffectsString(this);
    }
}
