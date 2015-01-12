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


    public static <K, V> boolean putAll(Map<K, Set<V>> map, K key, Collection<V> values) {
        return getMapSet(map, key).addAll(values);
    }

    public static <K, V> boolean put(Map<K, Set<V>> map, K key, V value) {
        return getMapSet(map, key).add(value);
    }

    public static <K, V> boolean containsEntry(Map<K, Set<V>> map, K key, V value) {
        Set<V> set = map.get(key);
        if (set == null) return false;
        return set.contains(value);
    }

    public static <K, V> Set<V> get(Map<K, Set<V>> map, K key) {
        Set<V> set = getMapSet(map, key);
        if (set == null) set = Collections.EMPTY_SET;
        return set;
    }

    private static <K, V> Set<V> getMapSet(Map<K, Set<V>> map, K key) {
        Set<V> set = map.get(key);
        if (set == null) {
            set = new HashSet<>();
            map.put(key, set);
        }
        assert set != null;
        return set;
    }


}
