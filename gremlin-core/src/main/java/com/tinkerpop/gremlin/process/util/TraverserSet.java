package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traverser;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Spliterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraverserSet<S> extends AbstractSet<Traverser.Admin<S>> implements Set<Traverser.Admin<S>>, Queue<Traverser.Admin<S>>, Serializable {

    private final Map<Traverser.Admin<S>, Traverser.Admin<S>> map = new LinkedHashMap<>();

    public TraverserSet() {

    }

    public TraverserSet(final Traverser.Admin<S> traverser) {
        this.map.put(traverser, traverser);
    }

    @Override
    public Iterator<Traverser.Admin<S>> iterator() {
        return this.map.keySet().iterator();
    }

    public Traverser.Admin<S> get(final Traverser.Admin<S> traverser) {
        return this.map.get(traverser);
    }

    @Override
    public int size() {
        return this.map.size();
    }

    public long bulkSize() {
        return this.map.values().stream().map(Traverser::bulk).reduce(0l, (a, b) -> a + b);
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
            existing.merge(traverser);
            return false;
        }
    }

    @Override
    public boolean offer(final Traverser.Admin<S> traverser) {
        return this.add(traverser);
    }

    @Override
    public Traverser.Admin<S> remove() {  // pop, exception if empty
        return this.map.remove(this.iterator().next());
    }

    @Override
    public Traverser.Admin<S> poll() {  // pop, null if empty
        return this.map.isEmpty() ? null : this.remove();
    }

    @Override
    public Traverser.Admin<S> element() { // peek, exception if empty
        return this.iterator().next();
    }

    @Override
    public Traverser.Admin<S> peek() { // peek, null if empty
        return this.map.isEmpty() ? null : this.iterator().next();
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

    @Override
    public String toString() {
        return this.map.keySet().toString();
    }

    public void sort(final Comparator<Traverser<S>> comparator) {
        final List<Traverser.Admin<S>> list = new ArrayList<>(this.map.keySet());
        Collections.sort(list, comparator);
        this.map.clear();
        list.forEach(traverser -> this.map.put(traverser, traverser));
    }

}
