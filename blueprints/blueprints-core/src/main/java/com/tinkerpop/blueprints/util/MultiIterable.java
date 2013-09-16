package com.tinkerpop.blueprints.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A helper class that is used to combine multiple iterables into a single closeable iterable.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MultiIterable<S> implements Iterable<S> {

    private final List<Iterable<S>> iterables;

    public MultiIterable(final List<Iterable<S>> iterables) {
        this.iterables = iterables;
    }

    public Iterator<S> iterator() {
        if (this.iterables.size() == 0) {
            return (Iterator) Collections.emptyList().iterator();
        } else {
            return new Iterator<S>() {
                private Iterator<S> currentIterator = iterables.get(0).iterator();
                private int current = 0;


                public void remove() {
                    currentIterator.remove();
                }

                public boolean hasNext() {
                    while (true) {
                        if (currentIterator.hasNext()) {
                            return true;
                        } else {
                            this.current++;
                            if (this.current >= iterables.size())
                                break;
                            this.currentIterator = iterables.get(this.current).iterator();
                        }
                    }
                    return false;
                }

                public S next() {
                    while (true) {
                        if (currentIterator.hasNext()) {
                            return currentIterator.next();
                        } else {
                            this.current++;
                            if (this.current >= iterables.size())
                                break;
                            this.currentIterator = iterables.get(current).iterator();
                        }
                    }
                    throw new NoSuchElementException();
                }
            };
        }
    }
}
