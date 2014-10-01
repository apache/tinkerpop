package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultSideEffects implements Traversal.SideEffects {

    private Map<String, Object> sideEffectsMap;
    private boolean isLocal = true;
    private ThreadLocal<Vertex> vertex = new ThreadLocal<>();


    @Override
    public boolean exists(final String key) {
        if (this.isLocal) {
            return (null != this.sideEffectsMap && this.sideEffectsMap.containsKey(key));
        } else {
            final Property<Map<String, Object>> property = vertex.get().property(DISTRIBUTED_SIDE_EFFECTS_VERTEX_PROPERTY_KEY);
            return property.isPresent() && property.value().containsKey(key);
        }
    }

    @Override
    public <V> void set(final String key, final V value) {
        SideEffectHelper.validateSideEffect(key, value);
        if (this.isLocal) {
            if (null == this.sideEffectsMap) this.sideEffectsMap = new HashMap<>();
            this.sideEffectsMap.put(key, value);
        } else {
            final Property<Map<String, Object>> property = vertex.get().property(DISTRIBUTED_SIDE_EFFECTS_VERTEX_PROPERTY_KEY);
            final Map<String, Object> sideEffects;
            if (!property.isPresent()) {
                sideEffects = new HashMap<>();
                vertex.get().property(DISTRIBUTED_SIDE_EFFECTS_VERTEX_PROPERTY_KEY, sideEffects);
            } else {
                sideEffects = property.value();
            }
            sideEffects.put(key, value);
        }
    }

    @Override
    public <V> V get(final String key) throws IllegalArgumentException {
        if (this.isLocal) {
            if (null == this.sideEffectsMap)
                throw Traversal.SideEffects.Exceptions.sideEffectDoesNotExist(key);
            else {
                final V v = (V) this.sideEffectsMap.get(key);
                if (null == v)
                    throw Traversal.SideEffects.Exceptions.sideEffectDoesNotExist(key);
                else
                    return v;
            }
        } else {
            final Property<Map<String, Object>> property = vertex.get().property(DISTRIBUTED_SIDE_EFFECTS_VERTEX_PROPERTY_KEY);
            if (!property.isPresent())
                throw Traversal.SideEffects.Exceptions.sideEffectDoesNotExist(key);
            else {
                final Map<String, Object> sideEffects = property.value();
                if (!sideEffects.containsKey(key))
                    throw Traversal.SideEffects.Exceptions.sideEffectDoesNotExist(key);
                else
                    return (V) sideEffects.get(key);
            }
        }
    }

    @Override
    public void remove(final String key) {
        if (this.isLocal) {
            if (null != this.sideEffectsMap) this.sideEffectsMap.remove(key);
        } else {

            final Property<Map<String, Object>> property = vertex.get().property(DISTRIBUTED_SIDE_EFFECTS_VERTEX_PROPERTY_KEY);
            if (property.isPresent())
                property.value().remove(key);
        }
    }

    @Override
    public Set<String> keys() {
        if (this.isLocal) {
            return null == this.sideEffectsMap ? Collections.emptySet() : this.sideEffectsMap.keySet();
        } else {
            final Property<Map<String, Object>> property = vertex.get().property(DISTRIBUTED_SIDE_EFFECTS_VERTEX_PROPERTY_KEY);
            return property.isPresent() ? property.value().keySet() : Collections.emptySet();
        }
    }

    public void setLocalVertex(final Vertex vertex) {
        this.isLocal = false;
        this.vertex.set(vertex);
    }
}
