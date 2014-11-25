package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.process.computer.Memory;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ImmutableMemory implements Memory.Admin {

    private final Memory baseMemory;

    public ImmutableMemory(final Memory baseMemory) {
        this.baseMemory = baseMemory;
    }

    @Override
    public Set<String> keys() {
        return this.baseMemory.keys();
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        return this.baseMemory.get(key);
    }

    @Override
    public void set(final String key, final Object value) {
        throw Memory.Exceptions.memoryIsCurrentlyImmutable();
    }

    @Override
    public int getIteration() {
        return this.baseMemory.getIteration();
    }

    @Override
    public long getRuntime() {
        return this.baseMemory.getRuntime();
    }

    @Override
    public long incr(final String key, final long delta) {
        throw Memory.Exceptions.memoryIsCurrentlyImmutable();
    }

    @Override
    public boolean and(final String key, final boolean bool) {
        throw Memory.Exceptions.memoryIsCurrentlyImmutable();
    }

    @Override
    public boolean or(final String key, final boolean bool) {
        throw Memory.Exceptions.memoryIsCurrentlyImmutable();
    }

    @Override
    public void incrIteration() {
        throw Memory.Exceptions.memoryIsCurrentlyImmutable();
    }

    @Override
    public void setRuntime(final long runtime) {
        throw Memory.Exceptions.memoryIsCurrentlyImmutable();
    }
}
