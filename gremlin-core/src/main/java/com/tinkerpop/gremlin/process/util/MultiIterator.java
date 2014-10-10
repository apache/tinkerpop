package com.tinkerpop.gremlin.process.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MultiIterator<T> implements Iterator<T> {

    private final List<Iterator<T>> iterators = new ArrayList<>();
    private int current = 0;
    private int limit;
    private int count = 0;

    public MultiIterator() {
        this(Integer.MAX_VALUE);
    }

    public MultiIterator(final int limit) {
        this.limit = limit;
    }

    public void addIterator(final Iterator<T> iterator) {
        this.iterators.add(iterator);
    }

    @Override
    public boolean hasNext() {
        if (this.count >= this.limit)
            return false;

        if (this.current >= this.iterators.size())
            return false;

        Iterator<T> currentIterator = iterators.get(this.current);

        while (true) {
            if (currentIterator.hasNext()) {
                return true;
            } else {
                this.current++;
                if (this.current >= iterators.size())
                    break;
                currentIterator = iterators.get(this.current);
            }
        }
        return false;
    }

    @Override
    public T next() {
        if (this.count >= this.limit)
            throw FastNoSuchElementException.instance();

        Iterator<T> currentIterator = iterators.get(this.current);
        while (true) {
            if (currentIterator.hasNext()) {
                this.count++;
                return currentIterator.next();
            } else {
                this.current++;
                if (this.current >= iterators.size())
                    break;
                currentIterator = iterators.get(current);
            }
        }
        throw FastNoSuchElementException.instance();
    }

    public void clear() {
        this.iterators.clear();
        this.current = 0;
        this.count = 0;
    }

}
