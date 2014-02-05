package com.tinkerpop.gremlin.pipes.filter;

import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.pipes.AbstractPipe;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FilterPipe<S> extends AbstractPipe<S, S> {

    protected Predicate<Holder<S>> predicate;

    public FilterPipe(final Gremlin pipeline, final Predicate<Holder<S>> predicate) {
        super(pipeline);
        this.predicate = predicate;
    }

    public FilterPipe(final Gremlin pipeline) {
        super(pipeline);
    }

    public void setPredicate(final Predicate<Holder<S>> predicate) {
        this.predicate = predicate;
    }

    public Holder<S> processNextStart() {
        while (true) {
            final Holder<S> holder = this.starts.next();
            if (this.predicate.test(holder)) return holder;
        }
    }
}
