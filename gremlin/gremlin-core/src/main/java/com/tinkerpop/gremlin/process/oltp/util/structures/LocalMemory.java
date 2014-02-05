package com.tinkerpop.gremlin.process.oltp.util.structures;

import com.tinkerpop.gremlin.process.Memory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LocalMemory implements Memory {

    private final Map<String, Object> variables = new HashMap<>();

    public <T> void set(final String variable, final T t) {
        this.variables.put(variable, t);
    }

    public <T> T get(final String variable) {
        return (T) this.variables.get(variable);
    }

    public Set<String> getVariables() {
        return this.variables.keySet();
    }

}
