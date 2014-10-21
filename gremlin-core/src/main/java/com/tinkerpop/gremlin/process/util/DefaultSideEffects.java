package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultSideEffects implements Traversal.SideEffects {

    private Map<String, Object> sideEffectMap = new HashMap<>();
    private Map<String, Supplier> withMap = new HashMap<>();

    public DefaultSideEffects() {

    }

    public DefaultSideEffects(final Vertex localVertex) {
        this.setLocalVertex(localVertex);
    }

    @Override
    public void setWith(final String key, final Supplier supplier) {
        this.withMap.put(key, supplier);
    }

    @Override
    public <V> Optional<Supplier<V>> getWith(final String key) {
        return Optional.ofNullable(this.withMap.get(key));
    }

    @Override
    public boolean exists(final String key) {
        return this.sideEffectMap.containsKey(key) || this.withMap.containsKey(key);
    }

    @Override
    public void set(final String key, final Object value) {
        SideEffectHelper.validateSideEffect(key, value);
        this.sideEffectMap.put(key, value);
    }

    @Override
    public <V> V get(final String key) throws IllegalArgumentException {
        final V value = (V) this.sideEffectMap.get(key);
        if (null != value)
            return value;
        else {
            if (this.withMap.containsKey(key)) {
                final V v = (V) this.withMap.get(key).get();
                this.sideEffectMap.put(key, v);
                return v;
            } else {
                throw Traversal.SideEffects.Exceptions.sideEffectDoesNotExist(key);
            }
        }
    }

    @Override
    public <V> V getOrCreate(final String key, final Supplier<V> orCreate) {
        if (this.sideEffectMap.containsKey(key))
            return (V) this.sideEffectMap.get(key);
        else if (this.withMap.containsKey(key)) {
            final V value = (V) this.withMap.get(key).get();
            this.sideEffectMap.put(key, value);
            return value;
        } else {
            final V value = orCreate.get();
            this.sideEffectMap.put(key, value);
            return value;
        }
    }

    @Override
    public void remove(final String key) {
        this.sideEffectMap.remove(key);
        this.withMap.remove(key);
    }

    @Override
    public Set<String> keys() {
        final Set<String> keys = new HashSet<>();
        keys.addAll(this.sideEffectMap.keySet());
        keys.addAll(this.withMap.keySet());
        return keys;
    }

    @Override
    public String toString() {
        return StringFactory.traversalSideEffectsString(this);
    }

    @Override
    public void setLocalVertex(final Vertex vertex) {
        final Property<Map<String, Object>> property = vertex.property(DISTRIBUTED_SIDE_EFFECTS_VERTEX_PROPERTY_KEY);
        if (property.isPresent()) {
            this.sideEffectMap = property.value();
        } else {
            this.sideEffectMap = new HashMap<>();
            vertex.property(DISTRIBUTED_SIDE_EFFECTS_VERTEX_PROPERTY_KEY, this.sideEffectMap);
        }
    }
}
