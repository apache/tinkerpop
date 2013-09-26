package com.tinkerpop.gremlin;

import java.util.Iterator;
import java.util.Spliterators;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinSpliterator<T> extends Spliterators.AbstractSpliterator<T> {

    private final Iterator<T> iterator;

    public GremlinSpliterator(final Iterable<T> iterable) {
        super(Integer.MAX_VALUE, 0);
        this.iterator = iterable.iterator();
    }

    public boolean tryAdvance(Consumer<? super T> action) {
        if (!iterator.hasNext())
            return false;

        action.accept(iterator.next());
        return true;
    }
}
