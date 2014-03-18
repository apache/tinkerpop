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

    public void addIterator(final Iterator<T> iterator) {
        this.iterators.add(iterator);
    }

    public boolean hasNext() {
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

    public T next() {
        Iterator<T> currentIterator = iterators.get(this.current);
        while (true) {
            if (currentIterator.hasNext()) {
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

}
