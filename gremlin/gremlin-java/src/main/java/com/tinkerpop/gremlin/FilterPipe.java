package com.tinkerpop.gremlin;

import com.tinkerpop.gremlin.util.GremlinHelper;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FilterPipe<S> extends AbstractPipe<S, S> {

    protected Predicate<Holder<S>> predicate;

    public FilterPipe(final Pipeline pipeline, final Predicate<Holder<S>> predicate) {
        super(pipeline);
        this.predicate = predicate;
    }

    public FilterPipe(final Pipeline pipeline) {
        super(pipeline);
    }

    public void setPredicate(final Predicate<Holder<S>> predicate) {
        this.predicate = predicate;
    }

    public Holder<S> processNextStart() {
        while (true) {
            final Holder<S> holder = this.starts.next();
            if (this.predicate.test(holder)) {
                holder.setFuture(GremlinHelper.getNextPipeLabel(this.pipeline, this));
                return holder;
            }
        }
    }
}
