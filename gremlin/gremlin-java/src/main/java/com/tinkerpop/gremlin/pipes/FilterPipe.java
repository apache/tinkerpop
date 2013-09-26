package com.tinkerpop.gremlin.pipes;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FilterPipe<S> extends AbstractPipe<S, S> {

    private final Predicate<S> predicate;

    public FilterPipe(final Predicate<S> predicate) {
        this.predicate = predicate;
    }

    public S processNextStart() {
        while (true) {
            final S s = this.starts.next();
            if (this.predicate.test(s))
                return s;
        }
    }
}
