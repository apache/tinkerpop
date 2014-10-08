package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traverser;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.Spliterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraverserSet<S> extends AbstractSet<Traverser.Admin<S>> implements Set<Traverser.Admin<S>>, Iterator<Traverser.Admin<S>> {

    private final LinkedHashMap<Traverser.Admin<S>, Traverser.Admin<S>> map = new LinkedHashMap<>();

    @Override
    public Iterator<Traverser.Admin<S>> iterator() {
        return this.map.keySet().iterator();
    }

    @Override
    public boolean hasNext() {
        return !this.map.isEmpty();
    }

    @Override
    public Traverser.Admin<S> next() {
        return this.map.remove(this.iterator().next());
    }

    public int size() {
        return this.map.size();
    }

    @Override
    public boolean isEmpty() {
        return this.map.isEmpty();
    }

    @Override
    public boolean contains(final Object traverser) {
        return this.map.containsKey(traverser);
    }

    @Override
    public boolean add(final Traverser.Admin<S> traverser) {
        final Traverser.Admin<S> existing = this.map.get(traverser);
        if (null == existing) {
            this.map.put(traverser, traverser);
            return true;
        } else {
            existing.setBulk(existing.getBulk() + traverser.getBulk());
            return false;
        }
    }

    @Override
    public boolean remove(final Object traverser) {
        return this.map.remove(traverser) != null;
    }

    @Override
    public void clear() {
        this.map.clear();
    }

    @Override
    public Spliterator<Traverser.Admin<S>> spliterator() {
        return this.map.keySet().spliterator();
    }

}
