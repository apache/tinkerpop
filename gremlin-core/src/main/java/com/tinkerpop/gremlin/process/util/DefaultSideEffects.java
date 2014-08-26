package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultSideEffects implements Traversal.SideEffects {

    private Map<String, Object> memory;

    public boolean exists(final String key) {
        return (null != this.memory && this.memory.containsKey(key));
    }

    public <T> void set(final String key, final T value) {
        SideEffectHelper.validateSideEffect(key, value);
        if (null == this.memory) this.memory = new HashMap<>();
        this.memory.put(key, value);
    }

    public <T> T get(final String key) throws IllegalArgumentException {
        if (null == this.memory)
            throw Traversal.SideEffects.Exceptions.sideEffectDoesNotExist(key);
        else {
            final T t = (T) this.memory.get(key);
            if (null == t)
                throw Traversal.SideEffects.Exceptions.sideEffectDoesNotExist(key);
            else
                return t;
        }
    }

    public void remove(final String key) {
        if (null != this.memory) this.memory.remove(key);
    }

    public Set<String> keys() {
        return null == this.memory ? Collections.emptySet() : this.memory.keySet();
    }

}
