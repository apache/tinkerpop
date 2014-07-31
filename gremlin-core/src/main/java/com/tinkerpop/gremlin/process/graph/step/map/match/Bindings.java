package com.tinkerpop.gremlin.process.graph.step.map.match;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
* @author Joshua Shinavier (http://fortytwo.net)
*/
public class Bindings<T> implements Comparable<Bindings<T>> {
    private final SortedMap<String, T> map = new TreeMap<>();

    public Bindings() {
    }

    public Bindings(final Map<String, T> map) {
        this.map.putAll(map);
    }

    public Bindings<T> put(final String name, final T value) {
        map.put(name, value);
        return this;
    }

    public int compareTo(Bindings<T> other) {
        int cmp = ((Integer) map.size()).compareTo(other.map.size());
        if (0 != cmp) return cmp;

        Iterator<Map.Entry<String, T>> i1 = map.entrySet().iterator();
        Iterator<Map.Entry<String, T>> i2 = other.map.entrySet().iterator();
        while (i1.hasNext()) {
            Map.Entry<String, T> e1 = i1.next();
            Map.Entry<String, T> e2 = i2.next();

            cmp = e1.getKey().compareTo(e1.getKey());
            if (0 != cmp) return cmp;

            cmp = e1.getValue().toString().compareTo(e2.getValue().toString());
            if (0 != cmp) return cmp;
        }

        return 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, T> entry : map.entrySet()) {
            if (first) first = false;
            else sb.append(", ");
            sb.append(entry.getKey()).append(":").append(entry.getValue());
        }
        sb.append("}");
        return sb.toString();
    }
}
