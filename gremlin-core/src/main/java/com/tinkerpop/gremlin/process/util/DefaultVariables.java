package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultVariables implements Traversal.Variables {

    private Map<String, Object> memory;

    public <T> void set(final String variable, final T value) {
        if (null == this.memory) this.memory = new HashMap<>();
        this.memory.put(variable, value);
    }

    public <T> T get(final String variable) {
        return null == this.memory ? null : (T) this.memory.get(variable);
    }

    public Set<String> getVariables() {
        return null == this.memory ? Collections.emptySet() : this.memory.keySet();
    }

}
