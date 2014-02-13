package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Memory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultMemory implements Memory {

    private final Map<String, Object> memory = new HashMap<>();

    public <T> void set(final String variable, final T value) {
        this.memory.put(variable, value);
    }

    public <T> T get(final String variable) {
        return (T) this.memory.get(variable);
    }

    public Set<String> getVariables() {
        return this.memory.keySet();
    }

}
