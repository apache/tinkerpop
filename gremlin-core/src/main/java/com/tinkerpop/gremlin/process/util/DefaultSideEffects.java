package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultSideEffects implements Traversal.SideEffects, Serializable {

    private Map<String, Object> sideEffects;

    @Override
    public boolean exists(final String key) {
        return (null != this.sideEffects && this.sideEffects.containsKey(key));
    }

    @Override
    public <V> void set(final String key, final V value) {
        SideEffectHelper.validateSideEffect(key, value);
        if (null == this.sideEffects) this.sideEffects = new HashMap<>();
        this.sideEffects.put(key, value);
    }

    @Override
    public <V> V get(final String key) throws IllegalArgumentException {
        if (null == this.sideEffects)
            throw Traversal.SideEffects.Exceptions.sideEffectDoesNotExist(key);
        else {
            final V t = (V) this.sideEffects.get(key);
            if (null == t)
                throw Traversal.SideEffects.Exceptions.sideEffectDoesNotExist(key);
            else
                return t;
        }
    }

    @Override
    public void remove(final String key) {
        if (null != this.sideEffects) this.sideEffects.remove(key);
    }

    @Override
    public Set<String> keys() {
        return null == this.sideEffects ? Collections.emptySet() : this.sideEffects.keySet();
    }
}
