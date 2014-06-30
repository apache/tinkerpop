package com.tinkerpop.gremlin.groovy.jsr223;

import org.codehaus.groovy.util.ManagedReference;
import org.codehaus.groovy.util.ReferenceBundle;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Expanded from the original {@code ManagedConcurrentHashMap} in groovy-core.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ManagedConcurrentValueMap<K, V> {
    private final ConcurrentHashMap<K, ManagedReference<V>> internalMap;
    private ReferenceBundle bundle;

    public ManagedConcurrentValueMap(final ReferenceBundle bundle) {
        this.bundle = bundle;
        internalMap = new ConcurrentHashMap<K, ManagedReference<V>>();
    }

    /**
     * Sets a new bundle used for reference creation. Be warned that
     * older entries will not be changed by this
     *
     * @param bundle - the ReferenceBundle
     */
    public void setBundle(final ReferenceBundle bundle) {
        this.bundle = bundle;
    }

    /**
     * Returns the value stored for the given key at the point of call.
     *
     * @param key a non null key
     * @return the value stored in the map for the given key
     */
    public V get(final K key) {
        final ManagedReference<V> ref = internalMap.get(key);
        if (ref != null) return ref.get();
        return null;
    }

    /**
     * Sets a new value for a given key. an older value is overwritten.
     *
     * @param key   a non null key
     * @param value the new value
     */
    public void put(final K key, final V value) {
        final ManagedReference<V> ref = new ManagedReference<V>(bundle, value) {
            @Override
            public void finalizeReference() {
                super.finalizeReference();
                internalMap.remove(key, get());
            }
        };
        internalMap.put(key, ref);
    }

    public void clear() {
        internalMap.clear();
    }
}