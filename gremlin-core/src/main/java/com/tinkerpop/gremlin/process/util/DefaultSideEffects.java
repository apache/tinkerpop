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

    private Map<String, Object> objectMap = new HashMap<>();
    private Map<String, Supplier> supplierMap = new HashMap<>();

    public DefaultSideEffects() {

    }

    public DefaultSideEffects(final Vertex localVertex) {
        this.setLocalVertex(localVertex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerSupplier(final String key, final Supplier supplier) {
        this.supplierMap.put(key, supplier);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> Optional<Supplier<V>> getRegisteredSupplier(final String key) {
        return Optional.ofNullable(this.supplierMap.get(key));
    }

    /**
     * {@inheritDoc}
     */
    public void registerSupplierIfAbsent(final String key, final Supplier supplier) {
        if (!this.supplierMap.containsKey(key))
            this.supplierMap.put(key, supplier);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(final String key) {
        return this.objectMap.containsKey(key) || this.supplierMap.containsKey(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(final String key, final Object value) {
        SideEffectHelper.validateSideEffect(key, value);
        this.objectMap.put(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> V get(final String key) throws IllegalArgumentException {
        final V value = (V) this.objectMap.get(key);
        if (null != value)
            return value;
        else {
            if (this.supplierMap.containsKey(key)) {
                final V v = (V) this.supplierMap.get(key).get();
                this.objectMap.put(key, v);
                return v;
            } else {
                throw Traversal.SideEffects.Exceptions.sideEffectDoesNotExist(key);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> V getOrCreate(final String key, final Supplier<V> orCreate) {
        if (this.objectMap.containsKey(key))
            return (V) this.objectMap.get(key);
        else if (this.supplierMap.containsKey(key)) {
            final V value = (V) this.supplierMap.get(key).get();
            this.objectMap.put(key, value);
            return value;
        } else {
            final V value = orCreate.get();
            this.objectMap.put(key, value);
            return value;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(final String key) {
        this.objectMap.remove(key);
        this.supplierMap.remove(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> keys() {
        final Set<String> keys = new HashSet<>();
        keys.addAll(this.objectMap.keySet());
        keys.addAll(this.supplierMap.keySet());
        return keys;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLocalVertex(final Vertex vertex) {
        final Property<Map<String, Object>> property = vertex.property(SIDE_EFFECTS);
        if (property.isPresent()) {
            this.objectMap = property.value();
        } else {
            this.objectMap = new HashMap<>();
            vertex.property(SIDE_EFFECTS, this.objectMap);
        }
    }

    @Override
    public String toString() {
        return StringFactory.traversalSideEffectsString(this);
    }
}
