package com.tinkerpop.gremlin;

import java.util.NoSuchElementException;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FilterMonad<S> extends Monad<S, S> {

    private final Predicate<S> predicate;

    public FilterMonad(Monad<?, S> start, Predicate<S> predicate) {
        super(start);
        this.predicate = predicate;
    }

    public S next() {
        S value = this.start.next();
        if (this.predicate.test(value))
            return value;
        else
            throw new NoSuchElementException();
    }

    public boolean hasNext() {
        return (this.start.hasNext()) ? this.predicate.test(this.start.next()) : false;
    }


}
