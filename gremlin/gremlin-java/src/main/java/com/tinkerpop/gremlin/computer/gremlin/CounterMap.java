package com.tinkerpop.gremlin.computer.gremlin;

import java.util.HashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CounterMap<K> extends HashMap<K, Long> {

    public void incrValue(final K key, final long increment) {
        this.put(key, increment + this.getOrDefault(key, 0l));
    }
}
