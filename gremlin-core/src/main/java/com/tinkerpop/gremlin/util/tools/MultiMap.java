package com.tinkerpop.gremlin.util.tools;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A number of static methods to interact with a multi map, i.e. a map that maps keys to sets of values.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class MultiMap {


    public static <K, V> boolean putAll(final Map<K, Set<V>> map, final K key, final Collection<V> values) {
        return getMapSet(map, key).addAll(values);
    }

    public static <K, V> boolean put(final Map<K, Set<V>> map, final K key, final V value) {
        return getMapSet(map, key).add(value);
    }

    public static <K, V> boolean containsEntry(final Map<K, Set<V>> map, final K key, final V value) {
        final Set<V> set = map.get(key);
        return set != null && set.contains(value);
    }

    public static <K, V> Set<V> get(final Map<K, Set<V>> map, final K key) {
        Set<V> set = getMapSet(map, key);
        if (set == null) set = Collections.emptySet();
        return set;
    }

    private static <K, V> Set<V> getMapSet(final Map<K, Set<V>> map, final K key) {
        Set<V> set = map.get(key);
        if (set == null) {
            set = new HashSet<>();
            map.put(key, set);
        }
        return set;
    }


}
