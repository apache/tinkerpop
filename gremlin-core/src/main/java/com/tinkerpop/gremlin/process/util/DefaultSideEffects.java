package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultSideEffects implements Traversal.SideEffects, Cloneable {

    private Map<String, Object> sideEffectsMap;

    @Override
    public boolean exists(final String key) {
        return (null != this.sideEffectsMap && this.sideEffectsMap.containsKey(key));
    }

    @Override
    public <V> void set(final String key, final V value) {
        SideEffectHelper.validateSideEffect(key, value);
        if (null == this.sideEffectsMap) this.sideEffectsMap = new HashMap<>();
        this.sideEffectsMap.put(key, value);
    }

    @Override
    public <V> V get(final String key) throws IllegalArgumentException {
        if (null == this.sideEffectsMap)
            throw Traversal.SideEffects.Exceptions.sideEffectDoesNotExist(key);
        else {
            final V t = (V) this.sideEffectsMap.get(key);
            if (null == t)
                throw Traversal.SideEffects.Exceptions.sideEffectDoesNotExist(key);
            else
                return t;
        }
    }

    @Override
    public void remove(final String key) {
        if (null != this.sideEffectsMap) this.sideEffectsMap.remove(key);
    }

    @Override
    public Set<String> keys() {
        return null == this.sideEffectsMap ? Collections.emptySet() : this.sideEffectsMap.keySet();
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        final DefaultSideEffects clone = (DefaultSideEffects) super.clone();
        if (null != this.sideEffectsMap) {
            clone.sideEffectsMap = new HashMap<>();
            this.sideEffectsMap.forEach((k, v) -> {
                clone.sideEffectsMap.put(k, v);
            });
        }
        return clone;
    }
}
