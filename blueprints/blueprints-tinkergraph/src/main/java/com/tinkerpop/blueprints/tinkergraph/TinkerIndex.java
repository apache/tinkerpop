package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Element;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerIndex<T extends Element> implements Serializable {

    protected Map<String, Map<Object, Set<T>>> index = new HashMap<>();
    protected final String indexName;
    protected final Class<T> indexClass;

    public TinkerIndex(final String indexName, final Class<T> indexClass) {
        this.indexName = indexName;
        this.indexClass = indexClass;
    }

    public String getIndexName() {
        return this.indexName;
    }

    public Class<T> getIndexClass() {
        return this.indexClass;
    }

    public void put(final String key, final Object value, final T element) {
        Map<Object, Set<T>> keyMap = this.index.get(key);
        if (keyMap == null) {
            keyMap = new HashMap<Object, Set<T>>();
            this.index.put(key, keyMap);
        }
        Set<T> objects = keyMap.get(value);
        if (null == objects) {
            objects = new HashSet<T>();
            keyMap.put(value, objects);
        }
        objects.add(element);

    }

    public Iterable<T> get(final String key, final Object value) {
        final Map<Object, Set<T>> keyMap = this.index.get(key);
        if (null == keyMap) {
            return Collections.emptyList();
        } else {
            Set<T> set = keyMap.get(value);
            if (null == set)
                return Collections.emptyList();
            else
                return new ArrayList<T>(set);
        }
    }

    public Iterable<T> query(final String key, final Object query) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    public long count(final String key, final Object value) {
        final Map<Object, Set<T>> keyMap = this.index.get(key);
        if (null == keyMap) {
            return 0;
        } else {
            Set<T> set = keyMap.get(value);
            if (null == set)
                return 0;
            else
                return set.size();
        }
    }

    public void remove(final String key, final Object value, final T element) {
        final Map<Object, Set<T>> keyMap = this.index.get(key);
        if (null != keyMap) {
            Set<T> objects = keyMap.get(value);
            if (null != objects) {
                objects.remove(element);
                if (objects.size() == 0) {
                    keyMap.remove(value);
                }
            }
        }
    }

    public void removeElement(final T element) {
        if (this.indexClass.isAssignableFrom(element.getClass())) {
            for (Map<Object, Set<T>> map : index.values()) {
                for (Set<T> set : map.values()) {
                    set.remove(element);
                }
            }
        }
    }
}
