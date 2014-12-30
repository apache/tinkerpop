package com.tinkerpop.gremlin.util.tools;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class MultiMap {


    public static<K,V> boolean putAll(Map<K,Set<V>> map, K key, Collection<V> values) {
        return getMapSet(map,key).addAll(values);
    }

    public static<K,V> boolean put(Map<K,Set<V>> map, K key, V value) {
        return getMapSet(map,key).add(value);
    }

    public static<K,V> boolean containsEntry(Map<K,Set<V>> map, K key, V value) {
        Set<V> set = map.get(key);
        if (set==null) return false;
        return set.contains(value);
    }

    private static<K,V> Set<V> getMapSet(Map<K,Set<V>> map, K key) {
        Set<V> set = map.get(key);
        if (set==null) {
            set = new HashSet<>();
            map.put(key,set);
        }
        assert set!=null;
        return set;
    }



}
