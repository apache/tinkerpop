package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultMemory implements Traversal.Memory {

    private Map<String, Object> memory;

    public <T> void set(final String key, final T value) {
        if (null == this.memory) this.memory = new HashMap<>();
        this.memory.put(key, value);
    }

    public <T> T get(final String key) {
        return null == this.memory ? null : (T) this.memory.get(key);
    }

    public Set<String> getKeys() {
        return null == this.memory ? Collections.emptySet() : this.memory.keySet();
    }

}
