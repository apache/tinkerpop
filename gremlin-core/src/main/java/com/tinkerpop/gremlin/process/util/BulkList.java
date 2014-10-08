package com.tinkerpop.gremlin.process.util;

import java.io.Serializable;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BulkList<S> extends AbstractList<S> implements List<S>, Serializable {

    private final Map<S, Long> map = new LinkedHashMap<>();

    @Override
    public int size() {
        return this.map.values().stream().collect(Collectors.summingLong(Long::longValue)).intValue();
    }

    @Override
    public boolean contains(final Object s) {
        return this.map.containsKey(s);
    }

    @Override
    public S get(final int index) {
        /*final Iterator<S> iterator = this.iterator();
        for (int i = 0; i < index; i++) {
            iterator().next();
        }
        return iterator.next();*/
        return this.toList().get(index);
    }

    @Override
    public boolean add(final S s) {
        return this.add(s, 1l);
    }

    public boolean add(final S s, final long bulk) {
        if (this.map.containsKey(s)) {
            this.map.put(s, this.map.get(s) + bulk);
            return false;
        } else {
            this.map.put(s, bulk);
            return true;
        }
    }

    public long get(final S s) {
        final Long bulk = this.map.get(s);
        return null == bulk ? 0 : bulk;
    }

    /*public void set(final S s, final long bulk) {
        this.map.remove(s);
        this.map.put(s, bulk);
    }*/

    @Override
    public boolean remove(final Object s) {
        return this.map.remove(s) != null;
    }

    @Override
    public void clear() {
        this.map.clear();
    }

    @Override
    public Spliterator<S> spliterator() {
        return this.toList().spliterator();
    }

    @Override
    public String toString() {
        return this.map.toString();
    }

    private List<S> toList() {
        final List<S> list = new ArrayList<>();
        this.map.forEach((k, v) -> {
            for (long i = 0; i < v; i++) {
                list.add(k);
            }
        });
        return list;
    }

    @Override
    public Iterator<S> iterator() {
        return new Iterator<S>() {
            final Iterator<Map.Entry<S, Long>> entryIterator = map.entrySet().iterator();
            S lastObject = null;
            long lastCount = 0l;

            public boolean hasNext() {
                return this.lastCount > 0l || this.entryIterator.hasNext();
            }

            @Override
            public S next() {
                if (this.lastCount > 0l) {
                    this.lastCount--;
                    return this.lastObject;
                }
                final Map.Entry<S, Long> entry = entryIterator.next();
                if (entry.getValue() == 1) {
                    return entry.getKey();
                } else {
                    this.lastObject = entry.getKey();
                    this.lastCount = entry.getValue() - 1;
                    return this.lastObject;
                }
            }
        };
    }
}
