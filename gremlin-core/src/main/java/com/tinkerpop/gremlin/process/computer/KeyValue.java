package com.tinkerpop.gremlin.process.computer;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class KeyValue<K, V> implements Serializable {

    private final K key;
    private final V value;
    private static final String EMPTY_STRING = "";
    private static final String TAB = "\t";
    private static final String NULL = "null";

    public KeyValue(final K key, final V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return this.key;
    }

    public V getValue() {
        return this.value;
    }

    public String toString() {
        if (this.key instanceof MapReduce.NullObject && this.value instanceof MapReduce.NullObject) {
            return EMPTY_STRING;
        } else if (this.key instanceof MapReduce.NullObject) {
            return makeString(this.value);
        } else if (this.value instanceof MapReduce.NullObject) {
            return makeString(this.key);
        } else {
            return makeString(this.key) + TAB + makeString(this.value);
        }
    }

    private static final String makeString(final Object object) {
        return null == object ? NULL : object.toString();
    }
}
